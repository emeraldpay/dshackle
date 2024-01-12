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

import io.emeraldpay.dshackle.BlockchainType
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.upstream.*
import io.emeraldpay.dshackle.upstream.generic.*
import io.emeraldpay.dshackle.upstream.bitcoin.BitcoinMultistream
import io.emeraldpay.dshackle.upstream.bitcoin.BitcoinRpcUpstream
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.calls.DefaultEthereumMethods
import io.emeraldpay.dshackle.upstream.ethereum.EthereumCachingReader
import io.emeraldpay.dshackle.upstream.ethereum.EthereumChainSpecific
import org.jetbrains.annotations.NotNull
import org.springframework.cloud.sleuth.brave.bridge.BraveTracer
import reactor.core.scheduler.Schedulers

class MultistreamHolderMock implements MultistreamHolder {

    private Map<Chain, DefaultEthereumMethods> target = [:]
    private Map<Chain, Multistream> upstreams = [:]

    MultistreamHolderMock(Chain chain, Upstream up) {
        addUpstream(chain, up)
    }

    Multistream addUpstream(@NotNull Chain chain, @NotNull Upstream up) {
        if (!upstreams.containsKey(chain)) {
            if (chain.type == BlockchainType.ETHEREUM) {
                if (up instanceof GenericMultistream) {
                    upstreams[chain] = up
                } else if (up instanceof GenericUpstream) {
                    upstreams[chain] = new GenericMultistream(
                            chain, Schedulers.immediate(), null, new ArrayList<Upstream>(), Caches.default(),
                            Schedulers.boundedElastic(),
                            EthereumChainSpecific.INSTANCE.makeCachingReaderBuilder(TestingCommons.tracerMock()),
                            EthereumChainSpecific.INSTANCE.&localReaderBuilder,
                            io.emeraldpay.dshackle.upstream.starknet.StarknetChainSpecific.INSTANCE.subscriptionBuilder(Schedulers.boundedElastic()),
                            null, Schedulers.immediate(),
                            TestingCommons.tracerMock()
                    )
                    upstreams[chain].addUpstream(up)
                } else {
                    throw new IllegalArgumentException("Unsupported upstream type ${up.class}")
                }
                upstreams[chain].start()
            } else if (chain.type == BlockchainType.BITCOIN) {
                if (up instanceof BitcoinMultistream) {
                    upstreams[chain] = up
                } else if (up instanceof BitcoinRpcUpstream) {
                    upstreams[chain] = new BitcoinMultistream(chain, [up as BitcoinRpcUpstream], Caches.default())
                } else {
                    throw new IllegalArgumentException("Unsupported upstream type ${up.class}")
                }
                upstreams[chain].start()
            }
        } else {
            upstreams[chain].addUpstream(up)
        }
        return upstreams[chain]
    }

    @Override
    Multistream getUpstream(@NotNull Chain chain) {
        return upstreams[chain]
    }

    @Override
    List<Chain> getAvailable() {
        return upstreams.keySet().toList()
    }

    @Override
    boolean isAvailable(@NotNull Chain chain) {
        return upstreams.containsKey(chain)
    }

    @Override
    List<Multistream> all() {
        return upstreams.values().toList()
    }

    static class EthereumMultistreamMock extends GenericMultistream {

        EthereumCachingReader customReader = null
        CallMethods customMethods = null
        Head customHead = null

        EthereumMultistreamMock(@NotNull Chain chain, @NotNull List<GenericUpstream> upstreams, @NotNull Caches caches) {
            super(chain, Schedulers.immediate(), null, upstreams, caches, Schedulers.boundedElastic(),
                    EthereumChainSpecific.INSTANCE.makeCachingReaderBuilder(new BraveTracer(null, null, null)),
                    EthereumChainSpecific.INSTANCE.&localReaderBuilder,
                    EthereumChainSpecific.INSTANCE.subscriptionBuilder(Schedulers.boundedElastic()), null, Schedulers.immediate(),
                    new BraveTracer(null, null, null))
        }

        EthereumMultistreamMock(@NotNull Chain chain, @NotNull List<GenericUpstream> upstreams) {
            this(chain, upstreams, Caches.default())
        }

        EthereumMultistreamMock(@NotNull Chain chain, @NotNull GenericUpstream upstream) {
            this(chain, [upstream])
        }

        @Override
        EthereumCachingReader getCachingReader() {
            if (customReader != null) {
                return customReader
            }
            return super.getCachingReader()
        }

        @Override
        CallMethods getMethods() {
            if (customMethods != null) {
                return customMethods
            }
            return super.getMethods()
        }

        @Override
        Head getHead() {
            if (customHead != null) {
                return customHead
            }
            return super.getHead()
        }

        @Override
        Head getHead(@NotNull Selector.Matcher mather) {
            if (customHead != null) {
                return customHead
            }
            return super.getHead(mather)
        }
    }

}
