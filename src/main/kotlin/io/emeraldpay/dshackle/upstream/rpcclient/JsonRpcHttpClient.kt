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
import io.emeraldpay.dshackle.reader.JsonRpcHttpReader
import io.emeraldpay.dshackle.upstream.ethereum.rpc.RpcException
import io.emeraldpay.dshackle.upstream.ethereum.rpc.RpcResponseError
import io.emeraldpay.dshackle.upstream.rpcclient.stream.AggregateResponse
import io.emeraldpay.dshackle.upstream.rpcclient.stream.JsonRpcStreamParser
import io.emeraldpay.dshackle.upstream.rpcclient.stream.Response
import io.emeraldpay.dshackle.upstream.rpcclient.stream.SingleResponse
import io.emeraldpay.dshackle.upstream.rpcclient.stream.StreamResponse
import io.micrometer.core.instrument.Metrics
import io.netty.buffer.Unpooled
import io.netty.handler.codec.http.HttpHeaderNames
import io.netty.handler.codec.http.HttpHeaders
import io.netty.handler.ssl.SslContextBuilder
import io.netty.resolver.DefaultAddressResolverGroup
import org.apache.commons.lang3.time.StopWatch
import reactor.core.publisher.Mono
import reactor.netty.http.client.HttpClient
import reactor.netty.resources.ConnectionProvider
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
) : JsonRpcHttpReader {

    private val parser = ResponseRpcParser()
    private val streamParser = JsonRpcStreamParser()
    private val httpClient: HttpClient

    init {
        val connectionProvider = ConnectionProvider.builder("dshackleConnectionPool")
            .maxConnections(1500)
            .pendingAcquireMaxCount(10000)
            .build()

        var build = HttpClient.create(connectionProvider)
            .compress(true)
            .resolver(DefaultAddressResolverGroup.INSTANCE)

        build = build.headers { h ->
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

    private fun execute(request: JsonRpcRequest): Mono<out Response> {
        val bytesRequest = request.toJson()

        val response = httpClient
            .post()
            .uri(target)
            .send(Mono.just(Unpooled.wrappedBuffer(bytesRequest)))

        return if (!request.isStreamed) {
            response.response { header, bytes ->
                val statusCode = header.status().code()

                bytes.aggregate().asByteArray().map {
                    AggregateResponse(it, statusCode)
                }
            }.single()
        } else {
            response.responseConnection { t, u ->
                streamParser.streamParse(
                    t.status().code(),
                    u.inbound().receive()
                        .asByteArray()
                        .doFinally {
                            u.dispose()
                        },
                )
            }.single()
        }
    }

    override fun onStop() {
        Metrics.globalRegistry.remove(metrics.timer)
        Metrics.globalRegistry.remove(metrics.fails)
    }

    override fun read(key: JsonRpcRequest): Mono<JsonRpcResponse> {
        val startTime = StopWatch()
        return Mono.just(key)
            .doOnNext {
                if (!startTime.isStarted) {
                    startTime.start()
                }
            }
            .flatMap(this@JsonRpcHttpClient::execute)
            .doOnNext {
                if (startTime.isStarted) {
                    metrics.timer.record(startTime.nanoTime, TimeUnit.NANOSECONDS)
                }
            }
            .transform(asJsonRpcResponse(key))
            .transform(convertErrors(key))
            .transform(throwIfError())
    }

    /**
     * The subscribers expect to catch an exception if the response contains JSON RPC Error. Convert it here to JsonRpcException
     */
    private fun throwIfError(): Function<Mono<JsonRpcResponse>, Mono<JsonRpcResponse>> {
        return Function { resp ->
            resp.flatMap {
                if (it.hasError()) {
                    Mono.error(JsonRpcUpstreamException(it.id, it.error!!))
                } else {
                    Mono.just(it)
                }
            }
        }
    }

    /**
     * Convert internal exceptions to standard JsonRpcException
     */
    private fun convertErrors(key: JsonRpcRequest): Function<Mono<JsonRpcResponse>, Mono<JsonRpcResponse>> {
        return Function { resp ->
            resp.onErrorResume { t ->
                val err = when (t) {
                    is RpcException -> JsonRpcException.from(t)
                    is JsonRpcException -> t
                    else -> JsonRpcException(key.id, t.message ?: t.javaClass.name, cause = t)
                }
                // here we're measure the internal errors, not upstream errors
                metrics.fails.increment()
                Mono.error(err)
            }
        }
    }

    /**
     * Process response from the upstream and convert it to JsonRpcResponse.
     * The input is a pair of (Http Status Code, Http Response Body)
     */
    private fun asJsonRpcResponse(key: JsonRpcRequest): Function<Mono<out Response>, Mono<JsonRpcResponse>> {
        return Function { resp ->
            resp.map {
                when (it) {
                    is AggregateResponse -> {
                        val parsed = parser.parse(it.response)
                        val statusCode = it.code
                        if (statusCode != 200) {
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
                        } else {
                            parsed
                        }
                    }
                    is StreamResponse -> {
                        JsonRpcResponse(it.stream, key.id)
                    }
                    is SingleResponse -> {
                        if (it.hasError()) {
                            JsonRpcResponse(null, it.error)
                        } else {
                            JsonRpcResponse(it.result, null)
                        }
                    }
                }
            }
        }
    }
}
