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

open class BitcoinRpcClient(
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