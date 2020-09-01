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

import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.startup.QuorumForLabels
import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.calls.DefaultBitcoinMethods
import io.emeraldpay.grpc.Chain
import org.slf4j.LoggerFactory

abstract class BitcoinUpstream(
        id: String,
        val chain: Chain,
        options: UpstreamsConfig.Options,
        role: UpstreamsConfig.UpstreamRole,
        callMethods: CallMethods,
        node: QuorumForLabels.QuorumItem
) : DefaultUpstream(id, options, role, callMethods, node) {

    constructor(id: String,
                chain: Chain,
                options: UpstreamsConfig.Options,
                role: UpstreamsConfig.UpstreamRole) : this(id, chain, options, role, DefaultBitcoinMethods(), QuorumForLabels.QuorumItem.empty())

    companion object {
        private val log = LoggerFactory.getLogger(BitcoinUpstream::class.java)
    }

}