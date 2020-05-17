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

import com.fasterxml.jackson.core.Version
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import io.emeraldpay.dshackle.FileResolver
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.cache.CachesFactory
import io.emeraldpay.dshackle.config.CacheConfig
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.calls.DirectCallMethods
import io.emeraldpay.dshackle.upstream.ethereum.EthereumMultistream
import io.emeraldpay.dshackle.upstream.ethereum.EthereumUpstream
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.rpc.JacksonRpcConverter

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

    static EthereumApiMock api() {
        return new EthereumApiMock(objectMapper())
    }

    static JacksonRpcConverter rpcConverter() {
        return new JacksonRpcConverter(objectMapper())
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

    static Multistream aggregatedUpstream(Reader<JsonRpcRequest, JsonRpcResponse> api) {
        return aggregatedUpstream(upstream(api))
    }

    static Multistream aggregatedUpstream(EthereumUpstream up) {
        return new EthereumMultistream(Chain.ETHEREUM, [up], Caches.default(objectMapper()), objectMapper()).tap {
            start()
        }
    }

    static CachesFactory emptyCaches() {
        return new CachesFactory(objectMapper(), new CacheConfig())
    }

    static FileResolver fileResolver() {
        return new FileResolver(new File("src/test/resources"))
    }
}
