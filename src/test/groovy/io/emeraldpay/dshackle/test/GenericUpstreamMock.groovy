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
import io.emeraldpay.dshackle.config.ChainsConfig.ChainConfig
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.foundation.ChainOptions
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.startup.QuorumForLabels
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.ChainResponse
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.calls.*
import io.emeraldpay.dshackle.upstream.generic.GenericUpstream
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundData
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundType
import org.jetbrains.annotations.NotNull
import org.reactivestreams.Publisher

class GenericUpstreamMock extends GenericUpstream {
    EthereumHeadMock ethereumHeadMock

    static CallMethods allMethods() {
        new AggregatedCallMethods([
                new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET, false),
                new DefaultBitcoinMethods(),
                new DirectCallMethods(["eth_test"])
        ])
    }

    GenericUpstreamMock(@NotNull Chain chain, @NotNull Reader<ChainRequest, ChainResponse> api) {
        this(chain, api, allMethods())
    }

    GenericUpstreamMock(@NotNull String id, @NotNull Chain chain, @NotNull Reader<ChainRequest, ChainResponse> api, Map<String, String> labels) {
        this(id, chain, api, allMethods(), labels)
    }

    GenericUpstreamMock(@NotNull String id, @NotNull Chain chain, @NotNull Reader<ChainRequest, ChainResponse> api) {
        this(id, chain, api, allMethods())
    }

    GenericUpstreamMock(@NotNull Chain chain, @NotNull Reader<ChainRequest, ChainResponse> api, CallMethods methods) {
        this("test", chain, api, methods)
    }

    GenericUpstreamMock(@NotNull String id, @NotNull Chain chain, @NotNull Reader<ChainRequest, ChainResponse> api, CallMethods methods) {
        this(id, chain, api, methods, Collections.<String, String>emptyMap())
    }

    GenericUpstreamMock(@NotNull String id, @NotNull Chain chain, @NotNull Reader<ChainRequest, ChainResponse> api, CallMethods methods, Map<String, String> labels) {
        super(id,
                chain,
                (byte)id.hashCode(),
                getOpts(),
                UpstreamsConfig.UpstreamRole.PRIMARY,
                methods,
                new QuorumForLabels.QuorumItem(1, UpstreamsConfig.Labels.fromMap(labels)),
                ChainConfig.default(),
                new ConnectorFactoryMock(api, new EthereumHeadMock()),
                io.emeraldpay.dshackle.upstream.starknet.StarknetChainSpecific.INSTANCE.&validator,
                io.emeraldpay.dshackle.upstream.starknet.StarknetChainSpecific.INSTANCE.&upstreamSettingsDetector,
                io.emeraldpay.dshackle.upstream.starknet.StarknetChainSpecific.INSTANCE.&lowerBoundService,
        )
        this.ethereumHeadMock = this.getHead() as EthereumHeadMock
        setLag(0)
        setStatus(UpstreamAvailability.OK)
        start()
    }

    static ChainOptions.Options getOpts() {
        def opt = ChainOptions.PartialOptions.getDefaults()
        opt.setDisableValidation(true)
        opt.setDisableUpstreamValidation(true)
        return opt.buildOptions()
    }

    void nextBlock(BlockContainer block) {
        this.ethereumHeadMock.nextBlock(block)
    }

    GenericConnectorMock getConnectorMock() {
        return this.connector as GenericConnectorMock
    }

    void setBlocks(Publisher<BlockContainer> blocks) {
        this.ethereumHeadMock.predefined = blocks
    }

    @Override
    String toString() {
        return "Upstream mock ${getId()}"
    }

    @Override
    Collection<LowerBoundData> getLowerBounds() {
        return List.of(new LowerBoundData(0, LowerBoundType.STATE))
    }

    @Override
    LowerBoundData getLowerBound(@NotNull LowerBoundType lowerBoundType) {
        return new LowerBoundData(0, LowerBoundType.STATE)
    }
}
