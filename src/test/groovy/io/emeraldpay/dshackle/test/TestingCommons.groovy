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


import io.emeraldpay.dshackle.FileResolver
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.cache.CachesFactory
import io.emeraldpay.dshackle.config.CacheConfig
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.reader.EmptyReader
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.calls.DirectCallMethods
import io.emeraldpay.dshackle.upstream.ethereum.EthereumMultistream
import io.emeraldpay.dshackle.upstream.ethereum.EthereumUpstream
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.domain.BlockHash
import io.infinitape.etherjar.rpc.json.BlockJson
import org.apache.commons.lang3.StringUtils

import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.time.temporal.TemporalUnit

class TestingCommons {

    static EthereumApiMock api() {
        return new EthereumApiMock()
    }

    static EthereumUpstreamMock upstream(String id, Reader<JsonRpcRequest, JsonRpcResponse> api) {
        return new EthereumUpstreamMock(id, Chain.ETHEREUM, api)
    }

    static EthereumUpstreamMock upstream(Reader<JsonRpcRequest, JsonRpcResponse> api) {
        return new EthereumUpstreamMock(Chain.ETHEREUM, api)
    }

    static EthereumUpstreamMock upstream(Reader<JsonRpcRequest, JsonRpcResponse> api, String method) {
        return upstream(api, [method])
    }

    static EthereumUpstreamMock upstream(Reader<JsonRpcRequest, JsonRpcResponse> api, List<String> methods) {
        return new EthereumUpstreamMock(Chain.ETHEREUM, api, new DirectCallMethods(methods))
    }

    static Multistream multistream(Reader<JsonRpcRequest, JsonRpcResponse> api) {
        return multistream(upstream(api))
    }

    static Multistream multistream(EthereumUpstream up) {
        return new EthereumMultistream(Chain.ETHEREUM, [up], Caches.default()).tap {
            start()
        }
    }

    static Multistream emptyMultistream() {
        return multistream(new EmptyReader<JsonRpcRequest, JsonRpcResponse>())
    }

    static CachesFactory emptyCaches() {
        return new CachesFactory(new CacheConfig())
    }

    static FileResolver fileResolver() {
        return new FileResolver(new File("src/test/resources"))
    }

    static BlockContainer blockForEthereum(Long height) {
        BlockJson block = new BlockJson().tap {
            setNumber(height)
            setHash(BlockHash.from("0xc4b01774e426325b50f0c709753ec7cf1f1774439d587dfb91f2a4eeb8179cde"))
            setTotalDifficulty(BigInteger.ONE)
            setTimestamp(predictableTimestamp(height, 14))
        }
        return BlockContainer.from(block)
    }

    static BlockContainer blockForBitcoin(Long height) {
        return new BlockContainer(
                height,
                BlockId.from(StringUtils.leftPad(height.toString(), 64, "0")),
                BigInteger.valueOf(height),
                predictableTimestamp(height, 60),
                false,
                null,
                null,
                []
        )
    }

    static Instant predictableTimestamp(Long x, int stepSeconds) {
        //start from 1 Jan 2020
        Instant.ofEpochSecond(1577876400)
                .plusSeconds(x * stepSeconds)
    }
}
