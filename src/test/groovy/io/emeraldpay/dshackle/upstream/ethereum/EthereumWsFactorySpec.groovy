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
package io.emeraldpay.dshackle.upstream.ethereum

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.cache.BlocksMemCache
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.test.TestingCommons
import io.infinitape.etherjar.domain.BlockHash
import io.infinitape.etherjar.rpc.ReactorRpcClient
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.json.TransactionRefJson
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit

class EthereumWsFactorySpec extends Specification {

    ObjectMapper objectMapper = TestingCommons.objectMapper()

    def "Fetch block"() {
        setup:
        def wsf = new EthereumWsFactory(new URI("http://localhost"), new URI("http://localhost"), objectMapper)
        def blocksCache = Mock(BlocksMemCache)

        def block = new BlockJson<TransactionRefJson>()
        block.number = 100
        block.hash = BlockHash.from("0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200")
        block.timestamp = Instant.now().truncatedTo(ChronoUnit.SECONDS)
        block.transactions = []
        block.uncles = []
        block.totalDifficulty = BigInteger.ONE

        def apiMock = TestingCommons.api()
        def upstream = TestingCommons.upstream(apiMock)
        def ws = wsf.create(upstream)

        apiMock.answerOnce("eth_getBlockByHash", ["0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200", false], block)

        when:
        ws.onNewBlock(block)

        then:
        StepVerifier.create(ws.flux.take(1))
                .expectNext(BlockContainer.from(block, objectMapper))
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }
}
