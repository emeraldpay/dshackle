/**
 * Copyright (c) 2020 EmeraldPay, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.emeraldpay.dshackle.upstream.rpcclient

import io.emeraldpay.dshackle.config.AuthConfig
import io.emeraldpay.dshackle.reader.StandardRpcReader
import io.emeraldpay.etherjar.rpc.RpcException
import io.emeraldpay.etherjar.rpc.RpcResponseError
import io.netty.buffer.Unpooled
import io.netty.handler.codec.http.HttpHeaderNames
import io.netty.handler.codec.http.HttpHeaders
import io.netty.handler.ssl.SslContextBuilder
import io.netty.resolver.DefaultAddressResolverGroup
import org.apache.commons.lang3.time.StopWatch
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import reactor.netty.http.client.HttpClient
import reactor.netty.resources.ConnectionProvider
import reactor.util.function.Tuple2
import reactor.util.function.Tuples
import java.io.ByteArrayInputStream
import java.security.KeyStore
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate
import java.util.Base64
import java.util.concurrent.TimeUnit
import java.util.function.Consumer
import java.util.function.Function

/**
 * JSON RPC client
 */
class JsonRpcHttpClient(
    private val target: String,
    private val metrics: RpcMetrics,
    basicAuth: AuthConfig.ClientBasicAuth? = null,
    tlsCAAuth: ByteArray? = null,
) : StandardRpcReader,
    WithHttpStatus {
    companion object {
        private val log = LoggerFactory.getLogger(JsonRpcHttpClient::class.java)

        // default connection pool has only 1000 of pending connections, which is not always enough
        private val connectionProvider =
            ConnectionProvider
                .builder("json-rpc-pool")
                .maxConnections(500)
                .pendingAcquireMaxCount(5000)
                .build()
    }

    private val parser = ResponseRpcParser()
    private val httpClient: HttpClient

    // some nodes respond with a corrupted response if compression is enabled, so disable it by default
    var compress: Boolean = false
    override var onHttpError: Consumer<Int>? = null

    init {
        var build =
            HttpClient
                .create(connectionProvider)
                .resolver(DefaultAddressResolverGroup.INSTANCE)
                .compress(compress)
                .doOnChannelInit(metrics.onChannelInit)

        build =
            build.headers { h ->
                h.add(HttpHeaderNames.CONTENT_TYPE, "application/json")
            }

        basicAuth?.let { auth ->
            val authString: String = auth.username + ":" + auth.password
            val authBase64 = Base64.getEncoder().encodeToString(authString.toByteArray())
            val encodedAuth = "Basic $authBase64"
            val headers = Consumer { h: HttpHeaders -> h.add(HttpHeaderNames.AUTHORIZATION, encodedAuth) }
            build = build.headers(headers)
        }

        tlsCAAuth?.let { auth ->
            val cf = CertificateFactory.getInstance("X.509")
            val cert = cf.generateCertificate(ByteArrayInputStream(auth)) as X509Certificate
            val ks = KeyStore.getInstance(KeyStore.getDefaultType())
            ks.load(null, "".toCharArray())
            ks.setCertificateEntry("server", cert)
            val sslContext = SslContextBuilder.forClient().trustManager(cert).build()

            build.secure { spec ->
                spec.sslContext(sslContext)
            }
        }

        this.httpClient = build
    }

    fun execute(request: ByteArray): Mono<Tuple2<Int, ByteArray>> {
        val response =
            httpClient
                .post()
                .uri(target)
                .send(Mono.just(request).map { Unpooled.wrappedBuffer(it) })

        return response
            .response { header, bytes ->
                val statusCode = header.status().code()
                bytes.aggregate().asByteArray().map {
                    Tuples.of(statusCode, it)
                }
            }.doFinally {
                metrics.onMessageFinished()
            }.single()
    }

    override fun read(key: JsonRpcRequest): Mono<JsonRpcResponse> {
        val startTime = StopWatch()
        return Mono
            .just(key)
            .doOnSubscribe { metrics.onMessageEnqueued() }
            .map(JsonRpcRequest::toJson)
            .doOnNext { startTime.start() }
            .flatMap(this@JsonRpcHttpClient::execute)
            .doOnNext {
                if (startTime.isStarted) {
                    metrics.timer.record(startTime.nanoTime, TimeUnit.NANOSECONDS)
                }
            }.transform(asJsonRpcResponse(key))
            .transform(metrics.processResponseSize)
            .transform(convertErrors(key))
            .transform(throwIfError())
    }

    /**
     * The subscribers expect to catch an exception if the response contains JSON RPC Error. Convert it here to JsonRpcException
     */
    private fun throwIfError(): Function<Mono<JsonRpcResponse>, Mono<JsonRpcResponse>> =
        Function { resp ->
            resp.flatMap {
                if (it.hasError()) {
                    val statusCode = if (it.httpCode == 200) null else it.httpCode
                    if (statusCode != null) {
                        onHttpError?.accept(statusCode)
                    }
                    Mono.error(JsonRpcException(it.id, it.error!!, statusCode))
                } else {
                    Mono.just(it)
                }
            }
        }

    /**
     * Convert internal exceptions to standard JsonRpcException
     */
    private fun convertErrors(key: JsonRpcRequest): Function<Mono<JsonRpcResponse>, Mono<JsonRpcResponse>> =
        Function { resp ->
            resp.onErrorResume { t ->
                val err =
                    when (t) {
                        is RpcException -> JsonRpcException.from(t)
                        is JsonRpcException -> t
                        else -> JsonRpcException(key.id, t.message ?: t.javaClass.name)
                    }
                // here we're measure the internal errors, not upstream errors
                metrics.fails.increment()
                Mono.error(err)
            }
        }

    /**
     * Process response from the upstream and convert it to JsonRpcResponse.
     * The input is a pair of (Http Status Code, Http Response Body)
     */
    private fun asJsonRpcResponse(key: JsonRpcRequest): Function<Mono<Tuple2<Int, ByteArray>>, Mono<JsonRpcResponse>> =
        Function { resp ->
            resp.map {
                val parsed = parser.parse(it.t2)
                val statusCode = it.t1
                if (statusCode != 200) {
                    val result =
                        if (parsed.hasError() && parsed.error!!.code != RpcResponseError.CODE_UPSTREAM_INVALID_RESPONSE) {
                            // extracted the error details from the HTTP Body
                            parsed
                        } else {
                            // here we got a valid response with ERROR as HTTP Status Code. We assume that HTTP Status has
                            // a higher priority so return an error here anyway
                            JsonRpcResponse.error(
                                RpcResponseError.CODE_UPSTREAM_INVALID_RESPONSE,
                                "HTTP Code: $statusCode",
                                JsonRpcResponse.NumberId(key.id),
                            )
                        }
                    result.copyWithHttpCode(statusCode)
                } else {
                    parsed
                }
            }
        }
}
