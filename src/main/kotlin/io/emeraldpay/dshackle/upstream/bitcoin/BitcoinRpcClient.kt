package io.emeraldpay.dshackle.upstream.bitcoin

import io.emeraldpay.dshackle.config.AuthConfig
import io.netty.buffer.Unpooled
import io.netty.handler.codec.http.HttpHeaderNames
import io.netty.handler.codec.http.HttpHeaders
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import reactor.netty.http.client.HttpClient
import java.util.*
import java.util.function.Consumer

class BitcoinRpcClient(
        private val target: String,
        basicAuth: AuthConfig.ClientBasicAuth?
) {

    companion object {
        private val log = LoggerFactory.getLogger(BitcoinRpcClient::class.java)
    }

    private val httpClient: HttpClient

    init {
        var build = HttpClient.create()

        build = build.headers { h ->
            h.add(HttpHeaderNames.CONTENT_TYPE, "application/json")
        }

        basicAuth?.let { basicAuth ->
            val authString: String = basicAuth.username + ":" + basicAuth.password
            val authBase64 = Base64.getEncoder().encodeToString(authString.toByteArray())
            val auth = "Basic $authBase64"
            val headers = Consumer { h: HttpHeaders -> h.add(HttpHeaderNames.AUTHORIZATION, auth) }
            build = build.headers(headers)
        }

        this.httpClient = build
    }

    fun execute(request: ByteArray): Mono<ByteArray> {
        val response = httpClient
                .post()
                .uri(target)
                .send(Mono.just(request).map { Unpooled.wrappedBuffer(it) })

        return response.responseContent()
                .aggregate()
                .asByteArray()
    }


}