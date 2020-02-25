/**
 * Copyright (c) 2019 ETCDEV GmbH
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

import com.fasterxml.jackson.core.Version
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.upstream.AggregatedUpstream
import io.emeraldpay.dshackle.upstream.ChainUpstreams
import io.emeraldpay.dshackle.upstream.calls.DirectCallMethods
import io.emeraldpay.dshackle.upstream.ethereum.DirectEthereumApi
import io.emeraldpay.dshackle.upstream.ethereum.EthereumUpstream
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.rpc.JacksonRpcConverter
import io.infinitape.etherjar.rpc.ReactorRpcClient

import java.text.SimpleDateFormat

class TestingCommons {

    static ObjectMapper objectMapper() {
        def module = new SimpleModule("EmeraldDShackle", new Version(1, 0, 0, null, null, null))

        def objectMapper = new ObjectMapper()
        objectMapper.registerModule(module)
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        objectMapper
                .setDateFormat(new SimpleDateFormat("yyyy-MM-dd\'T\'HH:mm:ss.SSS"))
                .setTimeZone(TimeZone.getTimeZone("UTC"))

        return objectMapper
    }

    static EthereumApiMock api(ReactorRpcClient rpcClient) {
        return new EthereumApiMock(rpcClient, objectMapper(), Chain.ETHEREUM)
    }

    static JacksonRpcConverter rpcConverter() {
        return new JacksonRpcConverter(objectMapper())
    }

    static EthereumUpstreamMock upstream(DirectEthereumApi api) {
        return new EthereumUpstreamMock(Chain.ETHEREUM, api)
    }

    static EthereumUpstreamMock upstream(DirectEthereumApi api, String method) {
        return upstream(api, [method])
    }

    static EthereumUpstreamMock upstream(DirectEthereumApi api, List<String> methods) {
        return new EthereumUpstreamMock(Chain.ETHEREUM, api, new DirectCallMethods(methods))
    }

    static AggregatedUpstream aggregatedUpstream(DirectEthereumApi api) {
        return aggregatedUpstream(upstream(api))
    }

    static AggregatedUpstream aggregatedUpstream(EthereumUpstream up) {
        return new ChainUpstreams(Chain.ETHEREUM, [up], Caches.default(), objectMapper())
    }
}
