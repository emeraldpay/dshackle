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

import io.emeraldpay.dshackle.upstream.BlockValidator
import io.emeraldpay.dshackle.upstream.forkchoice.ForkChoice
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcWsClient
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import reactor.core.Disposable
import reactor.core.publisher.Flux

class EthereumWsHead(
    private val ws: WsConnection,
    upstreamId: String,
    forkChoice: ForkChoice,
    blockValidator: BlockValidator
) : DefaultEthereumHead(upstreamId, forkChoice, blockValidator), Lifecycle {

    private val log = LoggerFactory.getLogger(EthereumWsHead::class.java)

    private var subscription: Disposable? = null

    override fun isRunning(): Boolean {
        return subscription != null
    }

    override fun start() {
        super.start()
        this.subscription?.dispose()
        val heads = Flux.merge(
            // get the current block, not just wait for the next update
            getLatestBlock(JsonRpcWsClient(ws)),
            ws.getBlocksFlux()
        )
        this.subscription = super.follow(heads)
    }

    override fun stop() {
        super.stop()
        subscription?.dispose()
        subscription = null
    }
}
