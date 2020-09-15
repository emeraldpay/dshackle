package io.emeraldpay.dshackle.upstream.bitcoin

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
import io.emeraldpay.dshackle.upstream.bitcoin.data.EsploraUnspent
import org.bitcoinj.core.Address
import org.bitcoinj.params.MainNetParams
import org.mockserver.integration.ClientAndServer
import org.mockserver.model.HttpRequest
import org.mockserver.model.HttpResponse
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Duration
import java.time.Instant

class EsploraClientSpec extends Specification {

    ClientAndServer mockServer

    def setup() {
        mockServer = ClientAndServer.startClientAndServer(23001);
    }

    def cleanup() {
        mockServer.stop()
    }

    def "get utxo"() {
        setup:
        def responseJson = this.class.getClassLoader().getResourceAsStream("bitcoin/esplora-utxo-1.json").text
        mockServer.when(
                HttpRequest.request()
                        .withMethod("GET")
                        .withPath("/address/35vktkPo4wdK8Twu4VMiuPLdCx23XEykGY/utxo")
        ).respond(
                HttpResponse.response(responseJson)
        )
        def client = new EsploraClient(new URI("http://localhost:23001"), null, null)
        when:
        def act = client.getUtxo(Address.fromString(new MainNetParams(), "35vktkPo4wdK8Twu4VMiuPLdCx23XEykGY"))

        then:
        StepVerifier.create(act)
                .expectNextMatches { list ->
                    def ok = list.size() == 14
                    if (!ok) println("invalid size")
                    ok = ok && list[0] == new EsploraUnspent("002eba7d9e8081afc687e1c3fa7b6a6451713b75bc91432d441bb1e7e1511c5c", 0, 105524, Instant.ofEpochSecond(1599808442), 647721)
                    if (!ok) println("invalid 0")
                    ok = ok && list[1] == new EsploraUnspent("1ee3cb3833b1e499e7f8babb1100c4aa0b4273f655036561d84dd72a5b197258", 0, 5248, Instant.ofEpochSecond(1600114632), 648312)
                    if (!ok) println("invalid 1")
                    ok = ok && list[12] == new EsploraUnspent("311d0d1d4eea6ee37d57954cd1002898bef64885d8e438afbbd7fe4fdc6e08de", 2250, 22978, Instant.ofEpochSecond(1599018027), 646382)
                    if (!ok) println("invalid 12")
                    ok = ok && list[13] == new EsploraUnspent("230317b8fdf1ae85f5fbdc49ca90851b1728c2d9432b2738d3fe1c6f68f046e4", 11, 8642, Instant.ofEpochSecond(1599273442), 646777)
                    if (!ok) println("invalid 13")
                    ok
                }
                .expectComplete()
                .verify(Duration.ofSeconds(3))

        when:
        def actTotal = client.getUtxo(Address.fromString(new MainNetParams(), "35vktkPo4wdK8Twu4VMiuPLdCx23XEykGY"))
                .block()
                .sum { it.value }

        then:
        actTotal == 309841L
    }

}
