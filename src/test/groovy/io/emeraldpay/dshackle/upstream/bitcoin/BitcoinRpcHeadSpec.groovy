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

import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Duration

class BitcoinRpcHeadSpec extends Specification {

    def "Follow 2 blocks created over 3 requests"() {
        setup:
        String hash1 = "1000000000000000000cf5a5d4dfc4347c0c1a863ec5fdb429b02b2162e50001"
        String hash2 = "2000000000000000000cf5a5d4dfc4347c0c1a863ec5fdb429b02b2162e50002"

        def block1 = """
            {
                "hash": "${hash1}",
                "confirmations": 2,
                "strippedsize": 800464,
                "size": 1591897,
                "weight": 3993289,
                "height": 626472,
                "version": 549453824,
                "versionHex": "20c00000",
                "merkleroot": "7835a26b6c9316232f8194c6b64b0a366e26a9a14e6a7e7409c34594d98a83c2",
                "tx": [],
                "time": 1587171526,
                "mediantime": 1587168930,
                "nonce": 4111305375,
                "bits": "171320bc",
                "difficulty": 14715214060656.53,
                "chainwork": "00000000000000000000000000000000000000000e9baec5f63190a2b0bbcf6b",
                "nTx": 0,
                "previousblockhash": "000000000000000000106b8cb739dd3c412613aa5d459a739794ec2572a74c10"
            }        
        """
        def block2 = """
            {
                "hash": "${hash2}",
                "confirmations": 2,
                "strippedsize": 800464,
                "size": 1591897,
                "weight": 3993289,
                "height": 626473,
                "version": 549453824,
                "versionHex": "20c00000",
                "merkleroot": "7835a26b6c9316232f8194c6b64b0a366e26a9a14e6a7e7409c34594d98a83c2",
                "tx": [],
                "time": 1587172526,
                "mediantime": 1587168930,
                "nonce": 4111305375,
                "bits": "171320bc",
                "difficulty": 14715214060656.53,
                "chainwork": "00000000000000000000000000000000000000000e9baec5f63190a2b0bbcf6c",
                "nTx": 0,
                "previousblockhash": "${hash1}"
            }        
        """

        def api = Mock(Reader) {
            _ * read(new JsonRpcRequest("getbestblockhash", new ListParams())) >>> [
                    Mono.just(new JsonRpcResponse("\"$hash1\"".bytes, null)),
                    Mono.just(new JsonRpcResponse("\"$hash1\"".bytes, null)),
                    Mono.just(new JsonRpcResponse("\"$hash2\"".bytes, null))
            ]
            _ * read(new JsonRpcRequest("getblock", new ListParams([hash1]))) >> Mono.just(new JsonRpcResponse(block1.bytes, null))
            _ * read(new JsonRpcRequest("getblock", new ListParams([hash2]))) >> Mono.just(new JsonRpcResponse(block2.bytes, null))
        }
        BitcoinRpcHead head = new BitcoinRpcHead(api, new ExtractBlock(), Duration.ofMillis(200), Schedulers.boundedElastic())

        when:
        def act = head.flux.take(2)
        head.start()

        then:
        StepVerifier.create(act)
                .expectNextMatches { block ->
                    block.hash.toHex() == hash1
                }.as("Block 1")
                .expectNextMatches { block ->
                    block.hash.toHex() == hash2
                }.as("Block 2")
                .expectComplete()
                .verify(Duration.ofSeconds(3))

    }
}
