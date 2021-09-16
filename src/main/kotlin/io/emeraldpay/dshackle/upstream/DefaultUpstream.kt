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
package io.emeraldpay.dshackle.upstream

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.startup.QuorumForLabels
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import reactor.core.publisher.Flux
import reactor.core.publisher.Sinks
import java.util.concurrent.atomic.AtomicReference

abstract class DefaultUpstream(
        private val id: String,
        defaultLag: Long,
        defaultAvail: UpstreamAvailability,
        private val options: UpstreamsConfig.Options,
        private val role: UpstreamsConfig.UpstreamRole,
        private val targets: CallMethods?,
        private val node: QuorumForLabels.QuorumItem?
) : Upstream {

    constructor(id: String, options: UpstreamsConfig.Options, role: UpstreamsConfig.UpstreamRole, targets: CallMethods?) :
            this(id, Long.MAX_VALUE, UpstreamAvailability.UNAVAILABLE, options, role, targets, QuorumForLabels.QuorumItem.empty())

    constructor(id: String, options: UpstreamsConfig.Options, role: UpstreamsConfig.UpstreamRole, targets: CallMethods?, node: QuorumForLabels.QuorumItem?) :
            this(id, Long.MAX_VALUE, UpstreamAvailability.UNAVAILABLE, options, role, targets, node)

    private val status = AtomicReference(Status(defaultLag, defaultAvail, statusByLag(defaultLag, defaultAvail)))
    private val statusStream = Sinks.many()
            .multicast()
            .directBestEffort<UpstreamAvailability>()

    override fun isAvailable(): Boolean {
        return getStatus() == UpstreamAvailability.OK
    }

    fun onStatus(value: BlockchainOuterClass.ChainStatus) {
        val available = value.availability
        val quorum = value.quorum
        setStatus(
                if (available != null) UpstreamAvailability.fromGrpc(available.number) else UpstreamAvailability.UNAVAILABLE
        )
    }

    override fun getStatus(): UpstreamAvailability {
        return status.get().status
    }

    fun setStatus(avail: UpstreamAvailability) {
        status.updateAndGet { curr ->
            Status(curr.lag, avail, statusByLag(curr.lag, avail))
        }
        statusStream.tryEmitNext(status.get().status)
    }

    fun statusByLag(lag: Long, proposed: UpstreamAvailability): UpstreamAvailability {
        return if (proposed == UpstreamAvailability.OK) {
            when {
                lag > 6 -> UpstreamAvailability.SYNCING
                lag > 1 -> UpstreamAvailability.LAGGING
                else -> proposed
            }
        } else proposed
    }

    override fun observeStatus(): Flux<UpstreamAvailability> {
        return statusStream.asFlux()
                .distinctUntilChanged()
    }

    override fun setLag(lag: Long) {
        if (lag < 0) {
            setLag(0)
        } else {
            status.updateAndGet { curr ->
                Status(lag, curr.avail, statusByLag(lag, curr.avail))
            }
            statusStream.tryEmitNext(status.get().status)
        }
    }

    override fun getLag(): Long {
        return this.status.get().lag
    }

    override fun getId(): String {
        return id
    }

    override fun getOptions(): UpstreamsConfig.Options {
        return options
    }

    override fun getRole(): UpstreamsConfig.UpstreamRole {
        return role;
    }

    override fun getMethods(): CallMethods {
        return targets ?: throw IllegalStateException("Methods are not set")
    }

    private val quorumByLabel = node?.let { QuorumForLabels(it) }
            ?: QuorumForLabels(QuorumForLabels.QuorumItem.empty())

    open fun getQuorumByLabel(): QuorumForLabels {
        return quorumByLabel
    }

    class Status(val lag: Long, val avail: UpstreamAvailability, val status: UpstreamAvailability)
}