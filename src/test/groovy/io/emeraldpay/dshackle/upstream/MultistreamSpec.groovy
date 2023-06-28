/**
 * Copyright (c) 2019 ETCDEV GmbH
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
package io.emeraldpay.dshackle.upstream

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.quorum.AlwaysQuorum
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.startup.UpstreamChangeEvent
import io.emeraldpay.dshackle.test.EthereumPosRpcUpstreamMock
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.upstream.calls.DirectCallMethods
import io.emeraldpay.dshackle.upstream.ethereum.EthereumPosMultiStream
import io.emeraldpay.dshackle.upstream.ethereum.EthereumPosUpstream
import io.emeraldpay.dshackle.upstream.ethereum.json.BlockJson
import io.emeraldpay.dshackle.upstream.grpc.EthereumPosGrpcUpstream
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.etherjar.domain.BlockHash
import io.emeraldpay.etherjar.rpc.json.TransactionRefJson
import org.jetbrains.annotations.NotNull
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit

class MultistreamSpec extends Specification {

    def "Aggregates methods"() {
        setup:
        def up1 = new EthereumPosRpcUpstreamMock("test1", Chain.ETHEREUM__MAINNET, TestingCommons.api(), new DirectCallMethods(["eth_test1", "eth_test2"]))
        def up2 = new EthereumPosRpcUpstreamMock("test1", Chain.ETHEREUM__MAINNET, TestingCommons.api(), new DirectCallMethods(["eth_test2", "eth_test3"]))
        def aggr = new EthereumPosMultiStream(Chain.ETHEREUM__MAINNET, [up1, up2], Caches.default(), Schedulers.parallel(), TestingCommons.tracerMock())
        when:
        aggr.onUpstreamsUpdated()
        def act = aggr.getMethods()
        then:
        act.isCallable("eth_test1")
        act.isCallable("eth_test2")
        act.isCallable("eth_test3")
        act.createQuorumFor("eth_test1") instanceof AlwaysQuorum
        act.createQuorumFor("eth_test2") instanceof AlwaysQuorum
        act.createQuorumFor("eth_test3") instanceof AlwaysQuorum
    }

    def "Filter Best Status accepts any input when none available "() {
        setup:
        def up = TestingCommons.upstream()
        def filter = new Multistream.FilterBestAvailability()
        def update1 = new Multistream.UpstreamStatus(
                up, UpstreamAvailability.LAGGING
        )
        when:
        def status = filter.apply(update1)
        then:
        status == UpstreamAvailability.LAGGING
    }

    def "Filter Best Status accepts better input"() {
        setup:
        def up1 = TestingCommons.upstream("test-1")
        def up2 = TestingCommons.upstream("test-2")
        def filter = new Multistream.FilterBestAvailability()
        def update0 = new Multistream.UpstreamStatus(
                up1, UpstreamAvailability.LAGGING
        )
        def update1 = new Multistream.UpstreamStatus(
                up2, UpstreamAvailability.OK
        )
        when:
        filter.apply(update0)
        def status = filter.apply(update1)
        then:
        status == UpstreamAvailability.OK
    }

    def "Filter Best Status declines worse input"() {
        setup:
        def up1 = TestingCommons.upstream("test-1")
        def up2 = TestingCommons.upstream("test-2")
        def filter = new Multistream.FilterBestAvailability()
        def update0 = new Multistream.UpstreamStatus(
                up1, UpstreamAvailability.LAGGING
        )
        def update1 = new Multistream.UpstreamStatus(
                up2, UpstreamAvailability.IMMATURE
        )
        when:
        filter.apply(update0)
        def status = filter.apply(update1)
        then:
        status == UpstreamAvailability.LAGGING
    }

    def "Filter Best Status accepts worse input from same upstream"() {
        setup:
        def up = TestingCommons.upstream("test-1")
        def filter = new Multistream.FilterBestAvailability()
        def update0 = new Multistream.UpstreamStatus(
                up, UpstreamAvailability.LAGGING
        )
        def update1 = new Multistream.UpstreamStatus(
                up, UpstreamAvailability.IMMATURE
        )
        when:
        filter.apply(update0)
        def status = filter.apply(update1)
        then:
        status == UpstreamAvailability.IMMATURE
    }

    def "Filter Best Status accepts any input if existing is outdated"() {
        setup:
        def up1 = TestingCommons.upstream("test-1")
        def up2 = TestingCommons.upstream("test-2")
        def filter = new Multistream.FilterBestAvailability()
        def update0 = new Multistream.UpstreamStatus(
                up1, UpstreamAvailability.OK
        )
        def update1 = new Multistream.UpstreamStatus(
                up2, UpstreamAvailability.IMMATURE
        )
        when:
        filter.apply(update0)
        def status = filter.apply(update1)
        then:
        status == UpstreamAvailability.OK
    }

    def "Filter Best Status declines same status"() {
        setup:
        def up1 = TestingCommons.upstream("test-1")
        def up2 = TestingCommons.upstream("test-2")
        def up3 = TestingCommons.upstream("test-3")
        def filter = new Multistream.FilterBestAvailability()
        def update0 = new Multistream.UpstreamStatus(
                up1, UpstreamAvailability.OK
        )
        def update1 = new Multistream.UpstreamStatus(
                up2, UpstreamAvailability.OK
        )
        def update2 = new Multistream.UpstreamStatus(
                up3, UpstreamAvailability.OK
        )

        when:
        filter.apply(update0)
        def status = filter.apply(update1)
        then:
        status == UpstreamAvailability.OK

        when:
        status = filter.apply(update2)
        then:
        status == UpstreamAvailability.OK
    }


    def "Filter upstream matching selector single"() {
        setup:
        def up1 = TestingCommons.upstream("test-1", "internal")
        def up2 = TestingCommons.upstream("test-2", "external")
        def up3 = TestingCommons.upstream("test-3", "external")
        def multistream = new EthereumPosMultiStream(Chain.ETHEREUM__MAINNET, [up1, up2, up3], Caches.default(), Schedulers.parallel(), TestingCommons.tracerMock())

        expect:
        multistream.getHead(new Selector.LabelMatcher("provider", ["internal"])).is(up1.ethereumHeadMock)
        multistream.getHead(new Selector.LabelMatcher("provider", ["unknown"])) in EmptyHead

        def head = multistream.getHead(new Selector.LabelMatcher("provider", ["external"]))
        head in MergedHead
        (head as MergedHead).isRunning()
        (head as MergedHead).getSources().sort() == [up2.ethereumHeadMock, up3.ethereumHeadMock].sort()

    }

    def "Proxy gRPC request - select one"() {
        setup:

        def call = BlockchainOuterClass.NativeSubscribeRequest.newBuilder()
                .setChainValue(Chain.ETHEREUM__MAINNET.id)
                .setMethod("newHeads")
                .build()

        def up1 = Mock(EthereumPosGrpcUpstream) {
            1 * isGrpc() >> true
            1 * getId() >> "internal"
            1 * getLabels() >> [UpstreamsConfig.Labels.fromMap(Collections.singletonMap("provider", "internal"))]
            1 * proxySubscribe(call) >> Flux.just("{}")
        }
        def up2 = Mock(EthereumPosGrpcUpstream) {
            1 * getId() >> "external"
            1 * getLabels() >> [UpstreamsConfig.Labels.fromMap(Collections.singletonMap("provider", "external"))]
        }
        def multiStream = new TestEthereumPosMultistream(Chain.ETHEREUM__MAINNET, [up1, up2], Caches.default())

        when:
        def act = multiStream.tryProxy(new Selector.LabelMatcher("provider", ["internal"]), call)

        then:
        StepVerifier.create(act)
                .expectNext("{}")
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Proxy gRPC request - not all gRPC"() {
        setup:

        def call = BlockchainOuterClass.NativeSubscribeRequest.newBuilder()
                .setChainValue(Chain.ETHEREUM__MAINNET.id)
                .setMethod("newHeads")
                .build()

        def up2 = Mock(EthereumPosGrpcUpstream) {
            1 * isGrpc() >> false
            1 * getId() >> "2"
            1 * getLabels() >> [UpstreamsConfig.Labels.fromMap(Collections.singletonMap("provider", "internal"))]
        }
        def multiStream = new TestEthereumPosMultistream(Chain.ETHEREUM__MAINNET, [up2], Caches.default())

        when:
        def act = multiStream.tryProxy(new Selector.LabelMatcher("provider", ["internal"]), call)

        then:
        !act
    }

    def "Change ms methods based on upstream availability"() {
        setup:
        def up1 = new EthereumPosRpcUpstreamMock("test1", Chain.ETHEREUM__MAINNET, TestingCommons.api(), new DirectCallMethods(["eth_test1", "eth_test2", "eth_test3"]))
        def up2 = new EthereumPosRpcUpstreamMock("test2", Chain.ETHEREUM__MAINNET, TestingCommons.api(), new DirectCallMethods(["eth_test1", "eth_test2"]))
        def ms = new EthereumPosMultiStream(Chain.ETHEREUM__MAINNET, new ArrayList<EthereumPosUpstream>(), Caches.default(), Schedulers.parallel(), TestingCommons.tracerMock())
        when:
        ms.onUpstreamChange(
                new UpstreamChangeEvent(Chain.ETHEREUM__MAINNET, up1, UpstreamChangeEvent.ChangeType.ADDED)
        )
        ms.onUpstreamChange(
                new UpstreamChangeEvent(Chain.ETHEREUM__MAINNET, up2, UpstreamChangeEvent.ChangeType.ADDED)
        )
        def states = ms.subscribeStateChanges()
        then:
        StepVerifier.create(states)
            .then {
                up1.onStatus(status(BlockchainOuterClass.AvailabilityEnum.AVAIL_UNAVAILABLE))
                up2.onStatus(status(BlockchainOuterClass.AvailabilityEnum.AVAIL_OK))
                up1.onStatus(status(BlockchainOuterClass.AvailabilityEnum.AVAIL_OK))
            }
                .expectNext(new Multistream.UpstreamChangeState(up1.getId(), UpstreamAvailability.UNAVAILABLE))
                .expectNext(new Multistream.UpstreamChangeState(up2.getId(), UpstreamAvailability.OK))
                .expectNext(new Multistream.UpstreamChangeState(up1.getId(), UpstreamAvailability.OK))
                .then {
                    assert ms.getMethods().supportedMethods == Set.of("eth_test1", "eth_test2", "eth_test3")
                }
                .then {
                    up1.onStatus(status(BlockchainOuterClass.AvailabilityEnum.AVAIL_SYNCING))
                }
                .expectNext(new Multistream.UpstreamChangeState(up1.getId(), UpstreamAvailability.SYNCING))
                .then {
                    assert ms.getMethods().supportedMethods == Set.of("eth_test1", "eth_test2")
                }
                .then {
                    up1.onStatus(status(BlockchainOuterClass.AvailabilityEnum.AVAIL_OK))
                }
                .expectNext(new Multistream.UpstreamChangeState(up1.getId(), UpstreamAvailability.OK))
                .then {
                    assert ms.getMethods().supportedMethods == Set.of("eth_test1", "eth_test2", "eth_test3")
                }
                .then {
                    up1.onStatus(status(BlockchainOuterClass.AvailabilityEnum.AVAIL_OK))
                }
                .expectNextCount(0)
                .then {
                    up2.onStatus(status(BlockchainOuterClass.AvailabilityEnum.AVAIL_OK))
                }
                .expectNextCount(0)
                .thenCancel()
                .verify(Duration.ofSeconds(3))

    }

    private BlockchainOuterClass.ChainStatus status(BlockchainOuterClass.AvailabilityEnum status) {
        return BlockchainOuterClass.ChainStatus.newBuilder()
                .setAvailability(status)
                .build()
    }

    class TestEthereumPosMultistream extends EthereumPosMultiStream {

        TestEthereumPosMultistream(@NotNull Chain chain, @NotNull List<EthereumPosUpstream> upstreams, @NotNull Caches caches) {
            super(chain, upstreams, caches, Schedulers.parallel(), TestingCommons.tracerMock())
        }

        @NotNull
        @Override
        Mono<Reader<JsonRpcRequest, JsonRpcResponse>> getLocalReader(boolean localEnabled) {
            return null
        }

        @Override
        Head getHead() {
            return null
        }

        public <T extends Upstream> T cast(Class<T> selfType) {
            return this
        }

        @Override
        ChainFees getFeeEstimation() {
            return null
        }

        @Override
        void init() {

        }
    }

    BlockContainer createBlock(long number) {
        def block = new BlockJson<TransactionRefJson>()
        block.number = number
        block.hash = BlockHash.from("0x0000000000000000000000000000000000000000000000000000000000" + number)
        block.totalDifficulty = BigInteger.ONE
        block.timestamp = Instant.now().truncatedTo(ChronoUnit.SECONDS)
        block.uncles = []
        block.transactions = []

        return BlockContainer.from(block)
    }
}
