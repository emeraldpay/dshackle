/**
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
package io.emeraldpay.dshackle.upstream.bitcoin

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.config.ChainsConfig
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.foundation.ChainOptions
import io.emeraldpay.dshackle.startup.QuorumForLabels
import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.calls.DefaultBitcoinMethods

abstract class BitcoinUpstream(
    id: String,
    val chain: Chain,
    options: ChainOptions.Options,
    role: UpstreamsConfig.UpstreamRole,
    callMethods: CallMethods,
    node: QuorumForLabels.QuorumItem,
    val esploraClient: EsploraClient? = null,
    chainConfig: ChainsConfig.ChainConfig,
) : DefaultUpstream(id, 0.toByte(), options, role, callMethods, node, chainConfig) {

    constructor(
        id: String,
        chain: Chain,
        options: ChainOptions.Options,
        role: UpstreamsConfig.UpstreamRole,
        chainConfig: ChainsConfig.ChainConfig,
    ) : this(id, chain, options, role, DefaultBitcoinMethods(), QuorumForLabels.QuorumItem.empty(), null, chainConfig)
}
