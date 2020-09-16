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

import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.config.AuthConfig
import io.emeraldpay.dshackle.upstream.bitcoin.data.EsploraUnspent
import io.netty.handler.codec.http.HttpHeaderNames
import io.netty.handler.codec.http.HttpHeaders
import io.netty.handler.ssl.SslContextBuilder
import org.bitcoinj.core.Address
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import reactor.netty.http.client.HttpClient
import java.io.ByteArrayInputStream
import java.net.URI
import java.security.KeyStore
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate
import java.util.*
import java.util.function.Consumer

class EsploraClient(
        private val url: URI,
        basicAuth: AuthConfig.ClientBasicAuth? = null,
        tlsCAAuth: ByteArray? = null
) {

    companion object {
        private val log = LoggerFactory.getLogger(EsploraClient::class.java)
    }

    private val httpClient: HttpClient

    init {
        var build = HttpClient.create()

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

    fun getUtxo(address: Address): Mono<List<EsploraUnspent>> {
        val response = httpClient
                .get()
                .uri("$url/address/$address/utxo")
        val json = parseResponse(response)

        return json.map {
            val parsed = Global.objectMapper.readerFor(EsploraUnspent::class.java)
                    .readValues<EsploraUnspent>(it);
            parsed.readAll()
        }
    }

    fun getTransactions(address: Address): Mono<List<Map<String, Any>>> {
        val response = httpClient
                .get()
                .uri("$url/address/$address/txs")
        val json = parseResponse(response)

        return json.map {
            val parsed = Global.objectMapper.readerFor(Map::class.java)
                    .readValues<Map<String, Any>>(it);
            parsed.readAll()
        }
    }

    private fun parseResponse(response: HttpClient.ResponseReceiver<*>): Mono<String> {
        return response
                .response { header, bytes ->
                    if (header.status().code() != 200) {
                        Mono.error(EsploraException("HTTP Code: ${header.status().code()} for ${header.fullPath()}"))
                    } else {
                        bytes.aggregate().asByteArray()
                    }
                }
                .single()
                .map { bytes -> String(bytes) }
    }

    class EsploraException(msg: String) : Exception(msg)

}