/**
 * Copyright (c) 2020 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.config.ChainsConfig
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.startup.QuorumForLabels
import io.emeraldpay.dshackle.upstream.Capability
import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.dshackle.upstream.calls.CallMethods

abstract class EthereumPosUpstream(
    id: String,
    hash: Byte,
    options: UpstreamsConfig.Options,
    role: UpstreamsConfig.UpstreamRole,
    targets: CallMethods?,
    private val node: QuorumForLabels.QuorumItem?,
    chainConfig: ChainsConfig.ChainConfig
) : DefaultUpstream(id, hash, options, role, targets, node, chainConfig) {

    private val capabilities = if (options.providesBalance != false) {
        setOf(Capability.RPC, Capability.BALANCE)
    } else {
        setOf(Capability.RPC)
    }

    override fun getCapabilities(): Set<Capability> {
        return capabilities
    }

    override fun getLabels(): Collection<UpstreamsConfig.Labels> {
        return node?.let { listOf(it.labels) } ?: emptyList()
    }

    abstract fun getIngressSubscription(): EthereumIngressSubscription
}
