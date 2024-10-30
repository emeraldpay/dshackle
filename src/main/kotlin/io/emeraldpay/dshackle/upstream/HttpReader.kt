package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.config.AuthConfig
import io.emeraldpay.dshackle.reader.ChainReader
import io.emeraldpay.dshackle.upstream.ethereum.rpc.RpcException
import io.micrometer.core.instrument.Metrics
import io.netty.handler.codec.http.HttpHeaderNames
import io.netty.handler.codec.http.HttpHeaders
import io.netty.handler.ssl.SslContextBuilder
import io.netty.resolver.DefaultAddressResolverGroup
import reactor.core.publisher.Mono
import reactor.netty.http.client.HttpClient
import reactor.netty.resources.ConnectionProvider
import java.io.ByteArrayInputStream
import java.security.KeyStore
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate
import java.time.Duration
import java.util.Base64
import java.util.function.Consumer
import java.util.function.Function

abstract class HttpReader(
    protected val target: String,
    protected val metrics: RequestMetrics?,
    basicAuth: AuthConfig.ClientBasicAuth? = null,
    tlsCAAuth: ByteArray? = null,
) : ChainReader {

    constructor() : this("", null)

    protected val httpClient: HttpClient

    init {
        val connectionProvider = ConnectionProvider.builder("dshackleConnectionPool")
            .maxConnections(1500)
            .pendingAcquireMaxCount(1000)
            .pendingAcquireTimeout(Duration.ofSeconds(10))
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

    override fun read(key: ChainRequest): Mono<ChainResponse> {
        return internalRead(key)
            .transform(convertErrors(key))
            .transform(throwIfError())
    }

    protected abstract fun internalRead(key: ChainRequest): Mono<ChainResponse>

    open fun onStop() {
        if (metrics != null) {
            Metrics.globalRegistry.remove(metrics.timer)
            Metrics.globalRegistry.remove(metrics.fails)
        }
    }

    /**
     * The subscribers expect to catch an exception if the response contains JSON RPC Error. Convert it here to JsonRpcException
     */
    private fun throwIfError(): Function<Mono<ChainResponse>, Mono<ChainResponse>> {
        return Function { resp ->
            resp.flatMap {
                if (it.hasError()) {
                    Mono.error(ChainCallUpstreamException(it.id, it.error!!))
                } else {
                    Mono.just(it)
                }
            }
        }
    }

    /**
     * Convert internal exceptions to standard JsonRpcException
     */
    private fun convertErrors(key: ChainRequest): Function<Mono<ChainResponse>, Mono<ChainResponse>> {
        return Function { resp ->
            resp.onErrorResume { t ->
                val err = when (t) {
                    is RpcException -> ChainException.from(t)
                    is ChainException -> t
                    else -> ChainException(key.id, t.message ?: t.javaClass.name, cause = t)
                }
                // here we're measure the internal errors, not upstream errors
                metrics?.fails?.increment()
                Mono.error(err)
            }
        }
    }
}
