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
package io.emeraldpay.dshackle.test

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.FileResolver
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.cache.CachesFactory
import io.emeraldpay.dshackle.config.CacheConfig
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.reader.EmptyReader
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.startup.UpstreamChangeEvent
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.calls.DirectCallMethods
import io.emeraldpay.dshackle.upstream.ethereum.EthereumChainSpecific
import io.emeraldpay.dshackle.upstream.ethereum.json.BlockJson
import io.emeraldpay.dshackle.upstream.generic.GenericMultistream
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.dshackle.upstream.ethereum.domain.BlockHash
import io.emeraldpay.dshackle.upstream.ethereum.domain.TransactionId
import io.emeraldpay.dshackle.upstream.ethereum.json.TransactionRefJson
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.logging.LoggingMeterRegistry
import org.apache.commons.lang3.StringUtils
import org.bouncycastle.jcajce.provider.digest.Keccak
import reactor.core.scheduler.Schedulers

import java.time.Instant

class TestingCommons {

    static ApiReaderMock api() {
        return new ApiReaderMock()
    }

    static TracerMock tracerMock() {
        return new TracerMock(null, null, null)
    }

    static GenericUpstreamMock upstream() {
        return new GenericUpstreamMock(Chain.ETHEREUM__MAINNET, api())
    }

    static GenericUpstreamMock upstream(String id) {
        return new GenericUpstreamMock(id, Chain.ETHEREUM__MAINNET, api())
    }

    static GenericUpstreamMock upstream(String id, String provider) {
        return new GenericUpstreamMock(id, Chain.ETHEREUM__MAINNET, api(), Collections.singletonMap("provider", provider))
    }

    static GenericUpstreamMock upstream(String id, Reader<JsonRpcRequest, JsonRpcResponse> api) {
        return new GenericUpstreamMock(id, Chain.ETHEREUM__MAINNET, api)
    }

    static GenericUpstreamMock upstream(Reader<JsonRpcRequest, JsonRpcResponse> api) {
        return new GenericUpstreamMock(Chain.ETHEREUM__MAINNET, api)
    }

    static GenericUpstreamMock upstream(Reader<JsonRpcRequest, JsonRpcResponse> api, String method) {
        return upstream(api, [method])
    }

    static GenericUpstreamMock upstream(Reader<JsonRpcRequest, JsonRpcResponse> api, List<String> methods) {
        return new GenericUpstreamMock(Chain.ETHEREUM__MAINNET, api, new DirectCallMethods(methods))
    }

    static GenericUpstreamMock upstream(Reader<JsonRpcRequest, JsonRpcResponse> api, CallMethods callMethods) {
        return new GenericUpstreamMock(Chain.ETHEREUM__MAINNET, api, callMethods)
    }

    static Multistream multistream(Reader<JsonRpcRequest, JsonRpcResponse> api) {
        return multistream(upstream(api))
    }

    static Multistream multistream(GenericUpstreamMock up) {
        return new GenericMultistream(Chain.ETHEREUM__MAINNET, Schedulers.immediate(), null, new ArrayList<Upstream>(), Caches.default(),
                Schedulers.boundedElastic(),
                EthereumChainSpecific.INSTANCE.makeCachingReaderBuilder(tracerMock()),
                EthereumChainSpecific.INSTANCE.&localReaderBuilder,
                EthereumChainSpecific.INSTANCE.subscriptionBuilder(Schedulers.boundedElastic()),
                null,
                Schedulers.immediate(),
                tracerMock()
        ).tap {
            it.processUpstreamsEvents(
                    new UpstreamChangeEvent(Chain.ETHEREUM__MAINNET, up, UpstreamChangeEvent.ChangeType.ADDED)
            )
        }
    }

    static Multistream emptyMultistream() {
        return multistream(new EmptyReader<JsonRpcRequest, JsonRpcResponse>())
    }

    static CachesFactory emptyCaches() {
        return new CachesFactory(new CacheConfig())
    }

    static List<Multistream> defaultMultistreams() {
        return [
                multistreamWithoutUpstreams(Chain.ETHEREUM__MAINNET),
                multistreamClassicWithoutUpstreams(Chain.ETHEREUM_CLASSIC__MAINNET)
        ]
    }

    static Multistream multistreamWithoutUpstreams(Chain chain) {
        return new GenericMultistream(chain, Schedulers.immediate(), null, [], emptyCaches().getCaches(chain), Schedulers.boundedElastic(),
                EthereumChainSpecific.INSTANCE.makeCachingReaderBuilder(tracerMock()),
                EthereumChainSpecific.INSTANCE.&localReaderBuilder,
                EthereumChainSpecific.INSTANCE.subscriptionBuilder(Schedulers.boundedElastic()),
                null,
                Schedulers.immediate(),
                tracerMock())
    }

    static Multistream multistreamClassicWithoutUpstreams(Chain chain) {
        return new GenericMultistream(chain, Schedulers.immediate(), null, [], emptyCaches().getCaches(chain), Schedulers.boundedElastic(),
                EthereumChainSpecific.INSTANCE.makeCachingReaderBuilder(tracerMock()),
                EthereumChainSpecific.INSTANCE.&localReaderBuilder,
                EthereumChainSpecific.INSTANCE.subscriptionBuilder(Schedulers.boundedElastic()),
                null,
                Schedulers.immediate(),
                tracerMock())
    }

    static FileResolver fileResolver() {
        return new FileResolver(new File("src/test/resources/configs"))
    }

    static BlockContainer blockForEthereum(Long height) {
        BlockJson block = new BlockJson().tap {
            setNumber(height)
            setParentHash(BlockHash.from("0xc4b01774e426325b50f0c709753ec7cf1f1774439d587dfb91f2a4eeb8179cde"))
            setHash(BlockHash.from((new Keccak.Digest256()).digest(height.byteValue())))
            setTotalDifficulty(BigInteger.ONE)
            setTimestamp(predictableTimestamp(height, 14))
        }
        return BlockContainer.from(block)
    }

    static BlockContainer enrichedBlockForEthereum(Long height) {
        BlockJson block = new BlockJson().tap {
            setNumber(height)
            setParentHash(BlockHash.from("0xc4b01774e426325b50f0c709753ec7cf1f1774439d587dfb91f2a4eeb8179cde"))
            setHash(BlockHash.from((new Keccak.Digest256()).digest(height.byteValue())))
            setTotalDifficulty(BigInteger.ONE)
            setTimestamp(predictableTimestamp(height, 14))
            setTransactions([new TransactionRefJson(TransactionId.from("0x3b23294ade15d39261245e6a3a53c3429a015891c95885b44ded29da2d60b29c"))])
        }
        return BlockContainer.from(block)
    }

    static BlockContainer blockForBitcoin(Long height) {
        def parent = BlockId.from(StringUtils.leftPad(height.toString(), 64, "0"))
        return new BlockContainer(
                height,
                BlockId.from(StringUtils.leftPad(height.toString(), 64, "0")),
                BigInteger.valueOf(height),
                predictableTimestamp(height, 60),
                false,
                null,
                null,
                parent,
                [],
                0,
                "upstream"
        )
    }

    static Instant predictableTimestamp(Long x, int stepSeconds) {
        //start from 1 Jan 2020
        Instant.ofEpochSecond(1577876400)
                .plusSeconds(x * stepSeconds)
    }

    static MeterRegistry meterRegistry = new LoggingMeterRegistry()
}
