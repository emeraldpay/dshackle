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
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.bitcoin.BitcoinMultistream
import io.emeraldpay.dshackle.upstream.bitcoin.BitcoinRpcUpstream
import io.emeraldpay.dshackle.upstream.calls.DefaultEthereumMethods
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.MultistreamHolder
import io.emeraldpay.dshackle.upstream.ethereum.EthereumMultistream
import io.emeraldpay.dshackle.upstream.ethereum.EthereumReader
import io.emeraldpay.dshackle.upstream.ethereum.EthereumUpstream
import io.emeraldpay.grpc.Chain
import org.jetbrains.annotations.NotNull
import reactor.core.publisher.Flux

class MultistreamHolderMock implements MultistreamHolder {

    private Map<Chain, DefaultEthereumMethods> target = [:]
    private Map<Chain, Multistream> upstreams = [:]

    MultistreamHolderMock(Chain chain, Upstream up) {
        addUpstream(chain, up)
    }

    Multistream addUpstream(@NotNull Chain chain, @NotNull Upstream up) {
        if (!upstreams.containsKey(chain)) {
            if (BlockchainType.fromBlockchain(chain) == BlockchainType.ETHEREUM) {
                if (up instanceof EthereumMultistream) {
                    upstreams[chain] = up
                } else if (up instanceof EthereumUpstream) {
                    upstreams[chain] = new EthereumMultistreamMock(chain, [up as EthereumUpstream], Caches.default())
                } else {
                    throw new IllegalArgumentException("Unsupported upstream type ${up.class}")
                }
                upstreams[chain].start()
            } else if (BlockchainType.fromBlockchain(chain) == BlockchainType.BITCOIN) {
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
    Flux<Chain> observeChains() {
        return Flux.fromIterable(getAvailable())
    }

    @Override
    DefaultEthereumMethods getDefaultMethods(@NotNull Chain chain) {
        if (target[chain] == null) {
            DefaultEthereumMethods targets = new DefaultEthereumMethods(chain)
            target[chain] = targets
        }
        return target[chain]
    }

    @Override
    boolean isAvailable(@NotNull Chain chain) {
        return upstreams.containsKey(chain)
    }

    static class EthereumMultistreamMock extends EthereumMultistream {

        EthereumReader customReader = null

        EthereumMultistreamMock(@NotNull Chain chain, @NotNull List<EthereumUpstream> upstreams, @NotNull Caches caches) {
            super(chain, upstreams, caches)
        }

        @Override
        EthereumReader getReader() {
            if (customReader != null) {
                return customReader
            }
            return super.getReader()
        }
    }

}
