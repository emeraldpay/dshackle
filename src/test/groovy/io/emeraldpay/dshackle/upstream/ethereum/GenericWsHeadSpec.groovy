/**
 * Copyright (c) 2022 EmeraldPay, Inc
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

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.test.GenericUpstreamMock
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.upstream.BlockValidator
import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.dshackle.upstream.ethereum.json.BlockJson
import io.emeraldpay.dshackle.upstream.forkchoice.AlwaysForkChoice
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.dshackle.upstream.ethereum.domain.BlockHash
import io.emeraldpay.dshackle.upstream.ethereum.json.TransactionRefJson
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.core.scheduler.Schedulers
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.atomic.AtomicReference

class GenericWsHeadSpec extends Specification {

    BlockHash parent = BlockHash.from("0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200")
    DefaultUpstream upstream = new GenericUpstreamMock(Chain.ETHEREUM__MAINNET, TestingCommons.api())

    def "Fetch block"() {
        setup:
        def block = new BlockJson<TransactionRefJson>()
        block.number = 100
        block.hash = BlockHash.from("0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200")
        block.parentHash = parent
        block.timestamp = Instant.now().truncatedTo(ChronoUnit.SECONDS)
        block.uncles = []
        block.totalDifficulty = BigInteger.ONE

        def headBlock = block.copy().with {
            Global.objectMapper.writeValueAsBytes(it)
        }

        def reader = Mock(Reader) {
            1 * it.read(new JsonRpcRequest("eth_getBlockByNumber", List.of("latest", false))) >> Mono.empty()
        }

        def ws = Mock(WsSubscriptions) {
            1 * it.connectionInfoFlux() >> Flux.empty()
        }

        1 * ws.subscribe(_) >> new WsSubscriptions.SubscribeData(
                Flux.fromIterable([headBlock]), "id", new AtomicReference<String>("")
        )

        def head = new GenericWsHead(new AlwaysForkChoice(), BlockValidator.ALWAYS_VALID, reader, ws, Schedulers.boundedElastic(), Schedulers.boundedElastic(), upstream, EthereumChainSpecific.INSTANCE)

        def res = BlockContainer.from(block)
        when:
        def act = head.getFlux()

        then:
        StepVerifier.create(act)
                .then {
                    head.start()
                }
                .expectNext(res)
                .thenCancel()
                .verify(Duration.ofSeconds(3))
    }

    def "Restart ethereum ws head"() {
        setup:
        def secondBlock = new BlockJson<TransactionRefJson>()
        secondBlock.parentHash = parent
        secondBlock.timestamp = Instant.now().truncatedTo(ChronoUnit.SECONDS)
        secondBlock.number = 105
        secondBlock.hash = BlockHash.from("0x29229361dc5aa1ec66c323dc7a299e2b61a8c8dd2a3522d41255ec10eca25dd8")

        def secondHeadBlock = secondBlock.with {
            Global.objectMapper.writeValueAsBytes(it)
        }

        def apiMock = TestingCommons.api()

        def connectionInfoSink = Sinks.many().multicast().directBestEffort()
        def ws = Mock(WsSubscriptions) {
            1 * it.connectionInfoFlux() >> connectionInfoSink.asFlux()
            2 * subscribe(_) >>> [
                    new WsSubscriptions.SubscribeData(Flux.error(new RuntimeException()), "id", new AtomicReference<String>("")),
                    new WsSubscriptions.SubscribeData(Flux.fromIterable([secondHeadBlock]), "id", new AtomicReference<String>(""))
            ]
        }

        def head = new GenericWsHead(new AlwaysForkChoice(), BlockValidator.ALWAYS_VALID, apiMock, ws, Schedulers.boundedElastic(), Schedulers.boundedElastic(), upstream, EthereumChainSpecific.INSTANCE)

        when:
        def act = head.getFlux()

        then:
        StepVerifier.create(act)
                .then {
                    head.start()
                }
                .expectNoEvent(Duration.ofMillis(100))
                .then {
                    head.onNoHeadUpdates()
                }
                .expectNext(BlockContainer.from(secondBlock))
                .thenCancel()
                .verify(Duration.ofSeconds(1))
    }

    def "Restart ethereum ws head immediately after reconnection"() {
        setup:
        def block = new BlockJson<TransactionRefJson>()
        block.timestamp = Instant.now().truncatedTo(ChronoUnit.SECONDS)
        block.number = 103
        block.parentHash = parent
        block.hash = BlockHash.from("0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200")
        def secondBlock = new BlockJson<TransactionRefJson>()
        secondBlock.parentHash = parent
        secondBlock.timestamp = Instant.now().truncatedTo(ChronoUnit.SECONDS)
        secondBlock.number = 105
        secondBlock.hash = BlockHash.from("0x29229361dc5aa1ec66c323dc7a299e2b61a8c8dd2a3522d41255ec10eca25dd8")

        def firstHeadBlock = block.with {
            Global.objectMapper.writeValueAsBytes(it)
        }
        def secondHeadBlock = secondBlock.with {
            Global.objectMapper.writeValueAsBytes(it)
        }

        def apiMock = TestingCommons.api()
        def connectionInfoSink = Sinks.many().multicast().directBestEffort()
        apiMock.answerOnce("eth_getBlockByHash", ["0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200", false], null)
        apiMock.answerOnce("eth_getBlockByHash", ["0x29229361dc5aa1ec66c323dc7a299e2b61a8c8dd2a3522d41255ec10eca25dd8", false], null)
        apiMock.answerOnce("eth_blockNumber", [], Mono.empty())
        apiMock.answerOnce("eth_blockNumber", [], Mono.empty())

        def ws = Mock(WsSubscriptions) {
            1 * it.connectionInfoFlux() >> connectionInfoSink.asFlux()
            2 * subscribe(_) >>> [
                    new WsSubscriptions.SubscribeData(Flux.fromIterable([firstHeadBlock]), "id", new AtomicReference<String>("")),
                    new WsSubscriptions.SubscribeData(Flux.fromIterable([secondHeadBlock]), "id", new AtomicReference<String>(""))
            ]
        }

        def head = new GenericWsHead(new AlwaysForkChoice(), BlockValidator.ALWAYS_VALID, apiMock, ws, Schedulers.boundedElastic(), Schedulers.boundedElastic(), upstream, EthereumChainSpecific.INSTANCE)

        when:
        def act = head.getFlux()

        then:
        StepVerifier.create(act)
                .then { head.start() }
                .expectNext(BlockContainer.from(block))
                .then { connectionInfoSink.tryEmitNext(new WsConnection.ConnectionInfo("id", WsConnection.ConnectionState.DISCONNECTED)) }
                .then { connectionInfoSink.tryEmitNext(new WsConnection.ConnectionInfo("id", WsConnection.ConnectionState.CONNECTED)) }
                .expectNext(BlockContainer.from(secondBlock))
                .thenCancel()
                .verify(Duration.ofSeconds(1))
    }

    def "No restart if new connection from pool has been connected"() {
        setup:
        def block = new BlockJson<TransactionRefJson>()
        block.timestamp = Instant.now().truncatedTo(ChronoUnit.SECONDS)
        block.number = 103
        block.parentHash = parent
        block.hash = BlockHash.from("0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200")

        def firstHeadBlock = block.with {
            Global.objectMapper.writeValueAsBytes(it)
        }

        def apiMock = TestingCommons.api()
        def connectionInfoSink = Sinks.many().multicast().directBestEffort()
        apiMock.answerOnce("eth_getBlockByHash", ["0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200", false], null)
        apiMock.answerOnce("eth_blockNumber", [], Mono.empty())

        def ws = Mock(WsSubscriptions) {
            1 * it.connectionInfoFlux() >> connectionInfoSink.asFlux()
            1 * subscribe(_) >>> [
                    new WsSubscriptions.SubscribeData(Flux.fromIterable([firstHeadBlock]), "id", new AtomicReference<String>("")),
            ]
        }

        def head = new GenericWsHead( new AlwaysForkChoice(), BlockValidator.ALWAYS_VALID, apiMock, ws, Schedulers.boundedElastic(), Schedulers.boundedElastic(), upstream, EthereumChainSpecific.INSTANCE)

        when:
        def act = head.getFlux()

        then:
        StepVerifier.create(act)
                .then { head.start() }
                .expectNext(BlockContainer.from(block))
                .then { connectionInfoSink.tryEmitNext(new WsConnection.ConnectionInfo("id", WsConnection.ConnectionState.CONNECTED)) }
                .expectNextCount(0)
                .thenCancel()
                .verify(Duration.ofSeconds(1))
    }

    def "No reset current subscription if it's already subscribed but other connection has been disconnected"() {
        setup:
        def block = new BlockJson<TransactionRefJson>()
        block.timestamp = Instant.now().truncatedTo(ChronoUnit.SECONDS)
        block.number = 103
        block.parentHash = parent
        block.hash = BlockHash.from("0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200")

        def firstHeadBlock = block.with {
            Global.objectMapper.writeValueAsBytes(it)
        }

        def apiMock = TestingCommons.api()
        def connectionInfoSink = Sinks.many().multicast().directBestEffort()
        apiMock.answerOnce("eth_getBlockByHash", ["0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200", false], null)
        apiMock.answerOnce("eth_blockNumber", [], Mono.empty())

        def ws = Mock(WsSubscriptions) {
            1 * it.connectionInfoFlux() >> connectionInfoSink.asFlux()
            1 * subscribe(_) >>> [
                    new WsSubscriptions.SubscribeData(Flux.fromIterable([firstHeadBlock]), "id", new AtomicReference<String>("")),
            ]
        }

        def head = new GenericWsHead(new AlwaysForkChoice(), BlockValidator.ALWAYS_VALID, apiMock, ws, Schedulers.boundedElastic(), Schedulers.boundedElastic(), upstream, EthereumChainSpecific.INSTANCE)

        when:
        def act = head.getFlux()

        then:
        StepVerifier.create(act)
                .then { head.start() }
                .expectNext(BlockContainer.from(block))
                .then {
                    connectionInfoSink.tryEmitNext(new WsConnection.ConnectionInfo("newId", WsConnection.ConnectionState.DISCONNECTED))
                    connectionInfoSink.tryEmitNext(new WsConnection.ConnectionInfo("newId", WsConnection.ConnectionState.CONNECTED))
                }
                .expectNextCount(0)
                .thenCancel()
                .verify(Duration.ofSeconds(1))
    }

    def "Reset current subscription if upstream is syncing and then restore it"() {
        setup:
        def block = new BlockJson<TransactionRefJson>()
        block.timestamp = Instant.now().truncatedTo(ChronoUnit.SECONDS)
        block.number = 103
        block.parentHash = parent
        block.hash = BlockHash.from("0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200")
        def secondBlock = new BlockJson<TransactionRefJson>()
        secondBlock.parentHash = parent
        secondBlock.timestamp = Instant.now().truncatedTo(ChronoUnit.SECONDS)
        secondBlock.number = 105
        secondBlock.hash = BlockHash.from("0x29229361dc5aa1ec66c323dc7a299e2b61a8c8dd2a3522d41255ec10eca25dd8")

        def firstHeadBlock = block.with {
            Global.objectMapper.writeValueAsBytes(it)
        }
        def secondHeadBlock = secondBlock.with {
            Global.objectMapper.writeValueAsBytes(it)
        }

        def apiMock = TestingCommons.api()
        def connectionInfoSink = Sinks.many().multicast().directBestEffort()
        apiMock.answerOnce("eth_getBlockByHash", ["0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200", false], null)
        apiMock.answerOnce("eth_blockNumber", [], Mono.empty())
        apiMock.answerOnce("eth_getBlockByHash", ["0x29229361dc5aa1ec66c323dc7a299e2b61a8c8dd2a3522d41255ec10eca25dd8", false], null)
        apiMock.answerOnce("eth_blockNumber", [], Mono.empty())

        def ws = Mock(WsSubscriptions) {
            1 * it.connectionInfoFlux() >> connectionInfoSink.asFlux()
            2 * subscribe(_) >>> [
                    new WsSubscriptions.SubscribeData(Flux.fromIterable([firstHeadBlock]), "id", new AtomicReference<String>("")),
                    new WsSubscriptions.SubscribeData(Flux.fromIterable([secondHeadBlock]), "id", new AtomicReference<String>("")),
            ]
        }

        def head = new GenericWsHead(new AlwaysForkChoice(), BlockValidator.ALWAYS_VALID, apiMock, ws, Schedulers.boundedElastic(), Schedulers.boundedElastic(), upstream, EthereumChainSpecific.INSTANCE)

        when:
        def act = head.getFlux()

        then:
        StepVerifier.create(act)
                .then { head.start() }
                .expectNext(BlockContainer.from(block))
                .then {
                    head.onSyncingNode(true)
                }
                .then {
                    assert !head.isRunning()
                }
                .then {
                    head.onNoHeadUpdates()
                }
                .then {
                    assert !head.isRunning()
                }
                .then {
                    head.onSyncingNode(false)
                    head.onNoHeadUpdates()
                }
                .expectNext(BlockContainer.from(secondBlock))
                .thenCancel()
                .verify(Duration.ofSeconds(1))
    }

    def "Unsubscribe if there is an error during subscription"() {
        setup:
        def block = new BlockJson<TransactionRefJson>()
        block.number = 100
        block.hash = BlockHash.from("0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200")
        block.parentHash = parent
        block.timestamp = Instant.now().truncatedTo(ChronoUnit.SECONDS)
        block.uncles = []
        block.totalDifficulty = BigInteger.ONE

        def reader = Mock(Reader) {
            1 * it.read(new JsonRpcRequest("eth_getBlockByNumber", List.of("latest", false))) >> Mono.empty()
        }
        def subId = "subId"
        def ws = Mock(WsSubscriptions) {
            1 * it.connectionInfoFlux() >> Flux.empty()
            1 * it.subscribe(_) >> new WsSubscriptions.SubscribeData(
                    Flux.error(new RuntimeException()), "id", new AtomicReference<String>(subId)
            )
            1 * it.unsubscribe(new JsonRpcRequest("eth_unsubscribe", List.of(subId), 2, null, null, false)) >>
                    Mono.just(new JsonRpcResponse("".bytes, null))
        }

        def head = new GenericWsHead(new AlwaysForkChoice(), BlockValidator.ALWAYS_VALID, reader, ws, Schedulers.boundedElastic(), Schedulers.boundedElastic(), upstream, EthereumChainSpecific.INSTANCE)

        when:
        def act = head.getFlux()

        then:
        StepVerifier.create(act)
            .then {
                head.start()
            }
            .expectNoEvent(Duration.ofMillis(100))
            .thenCancel()
            .verify(Duration.ofSeconds(3))
    }

    def "If there is ws disconnect then head must emit false its liveness state"() {
        setup:
        def secondBlock = new BlockJson<TransactionRefJson>()
        secondBlock.parentHash = parent
        secondBlock.timestamp = Instant.now().truncatedTo(ChronoUnit.SECONDS)
        secondBlock.number = 105
        secondBlock.hash = BlockHash.from("0x29229361dc5aa1ec66c323dc7a299e2b61a8c8dd2a3522d41255ec10eca25dd8")

        def secondHeadBlock = secondBlock.with {
            Global.objectMapper.writeValueAsBytes(it)
        }

        def apiMock = TestingCommons.api()

        def connectionInfoSink = Sinks.many().multicast().directBestEffort()
        def ws = Mock(WsSubscriptions) {
            1 * it.connectionInfoFlux() >> connectionInfoSink.asFlux()
            1 * subscribe(_) >>> [
                    new WsSubscriptions.SubscribeData(Flux.fromIterable([secondHeadBlock]), "id", new AtomicReference<String>(""))
            ]
        }

        def head = new GenericWsHead(new AlwaysForkChoice(), BlockValidator.ALWAYS_VALID, apiMock, ws, Schedulers.boundedElastic(), Schedulers.boundedElastic(), upstream, EthereumChainSpecific.INSTANCE)

        when:
        head.start()
        def liveness = head.headLiveness()

        then:
        StepVerifier.create(liveness)
                .then {
                    connectionInfoSink.tryEmitNext(new WsConnection.ConnectionInfo("id", WsConnection.ConnectionState.DISCONNECTED))
                }
                .expectNext(false)
                .thenCancel()
                .verify(Duration.ofSeconds(1))
    }
}
