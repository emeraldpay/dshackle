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


import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.upstream.ForkWatch
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.calls.AggregatedCallMethods
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.startup.QuorumForLabels
import io.emeraldpay.dshackle.upstream.calls.DefaultBitcoinMethods
import io.emeraldpay.dshackle.upstream.calls.DefaultEthereumMethods
import io.emeraldpay.dshackle.upstream.calls.DirectCallMethods
import io.emeraldpay.dshackle.upstream.ethereum.EthereumRpcUpstream
import io.emeraldpay.dshackle.upstream.ethereum.EthereumUpstream
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.grpc.Chain
import org.jetbrains.annotations.NotNull
import org.reactivestreams.Publisher

class EthereumUpstreamMock extends EthereumRpcUpstream {

    EthereumHeadMock ethereumHeadMock = new EthereumHeadMock()
    UpstreamAvailability mockStatus = null

    static CallMethods allMethods() {
        new AggregatedCallMethods([
                new DefaultEthereumMethods(Chain.ETHEREUM),
                new DefaultBitcoinMethods(),
                new DirectCallMethods(["eth_test"])
        ])
    }

    EthereumUpstreamMock(@NotNull Chain chain, @NotNull Reader<JsonRpcRequest, JsonRpcResponse> api) {
        this(chain, api, allMethods())
    }

    EthereumUpstreamMock(@NotNull String id, @NotNull Chain chain, @NotNull Reader<JsonRpcRequest, JsonRpcResponse> api) {
        this(id, chain, api, allMethods())
    }

    EthereumUpstreamMock(@NotNull Chain chain, @NotNull Reader<JsonRpcRequest, JsonRpcResponse> api, CallMethods methods) {
        this("test", chain, api, methods)
    }

    EthereumUpstreamMock(@NotNull String id, @NotNull Chain chain, @NotNull Reader<JsonRpcRequest, JsonRpcResponse> api, CallMethods methods) {
        super(id, chain, new ForkWatch.Never(), api, null,
                UpstreamsConfig.Options.getDefaults(),
                UpstreamsConfig.UpstreamRole.PRIMARY,
                new QuorumForLabels.QuorumItem(1, new UpstreamsConfig.Labels()),
                methods)
        setLag(0)
        setStatus(UpstreamAvailability.OK)
        start()
    }

    void nextBlock(BlockContainer block) {
        ethereumHeadMock.nextBlock(block)
    }

    void setBlocks(Publisher<BlockContainer> blocks) {
        ethereumHeadMock.predefined = blocks
    }

    @Override
    UpstreamAvailability getStatus() {
        if (mockStatus != null) {
            return mockStatus
        }
        return super.getStatus()
    }

    @Override
    Head createHead() {
        return ethereumHeadMock
    }

    @Override
    Head getHead() {
        return ethereumHeadMock
    }

    @Override
    String toString() {
        return "Upstream mock ${getId()}"
    }
}
