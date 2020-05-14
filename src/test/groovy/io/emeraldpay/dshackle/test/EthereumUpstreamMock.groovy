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

import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.startup.QuorumForLabels
import io.emeraldpay.dshackle.upstream.calls.DefaultEthereumMethods
import io.emeraldpay.dshackle.upstream.ethereum.EthereumUpstream
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.grpc.Chain
import org.jetbrains.annotations.NotNull
import org.reactivestreams.Publisher

class EthereumUpstreamMock extends EthereumUpstream {

    EthereumHeadMock ethereumHeadMock = new EthereumHeadMock()

    EthereumUpstreamMock(@NotNull Chain chain, @NotNull Reader<JsonRpcRequest, JsonRpcResponse> api) {
        this(chain, api, new DefaultEthereumMethods(TestingCommons.objectMapper(), chain))
    }

    EthereumUpstreamMock(@NotNull String id, @NotNull Chain chain, @NotNull Reader<JsonRpcRequest, JsonRpcResponse> api) {
        this(id, chain, api, new DefaultEthereumMethods(TestingCommons.objectMapper(), chain))
    }

    EthereumUpstreamMock(@NotNull Chain chain, @NotNull Reader<JsonRpcRequest, JsonRpcResponse> api, CallMethods methods) {
        this("test", chain, api, methods)
    }

    EthereumUpstreamMock(@NotNull String id, @NotNull Chain chain, @NotNull Reader<JsonRpcRequest, JsonRpcResponse> api, CallMethods methods) {
        super(id, chain, api, null,
                UpstreamsConfig.Options.getDefaults(), new QuorumForLabels.QuorumItem(1, new UpstreamsConfig.Labels()),
                methods, TestingCommons.objectMapper())
        setLag(0)
        setStatus(UpstreamAvailability.OK)
    }

    void nextBlock(BlockContainer block) {
        ethereumHeadMock.nextBlock(block)
    }

    void setBlocks(Publisher<BlockContainer> blocks) {
        ethereumHeadMock.predefined = blocks
    }

    @Override
    Head createHead() {
        return ethereumHeadMock
    }

    @Override
    Head getHead() {
        return ethereumHeadMock
    }
}
