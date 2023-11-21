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

import io.emeraldpay.api.Chain
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.quorum.CallQuorum
import io.emeraldpay.dshackle.startup.QuorumForLabels
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import org.springframework.context.Lifecycle
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.core.scheduler.Schedulers
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Consumer

abstract class DefaultUpstream(
    private val id: String,
    private val chain: Chain,
    defaultLag: Long,
    defaultAvail: UpstreamAvailability,
    private val forkWatch: ForkWatch,
    private val options: UpstreamsConfig.Options,
    private val role: UpstreamsConfig.UpstreamRole,
    private val targets: CallMethods?,
    private val node: QuorumForLabels.QuorumItem?
) : Upstream, Lifecycle {

    constructor(
        id: String,
        chain: Chain,
        forkWatch: ForkWatch,
        options: UpstreamsConfig.Options,
        role: UpstreamsConfig.UpstreamRole,
        targets: CallMethods?
    ) :
        this(
            id,
            chain,
            Long.MAX_VALUE,
            UpstreamAvailability.UNAVAILABLE,
            forkWatch,
            options,
            role,
            targets,
            QuorumForLabels.QuorumItem.empty()
        )

    constructor(
        id: String,
        chain: Chain,
        forkWatch: ForkWatch,
        options: UpstreamsConfig.Options,
        role: UpstreamsConfig.UpstreamRole,
        targets: CallMethods?,
        node: QuorumForLabels.QuorumItem?
    ) :
        this(id, chain, Long.MAX_VALUE, UpstreamAvailability.UNAVAILABLE, forkWatch, options, role, targets, node)

    private val status = AtomicReference(Status(defaultLag, defaultAvail, statusByLag(defaultLag, defaultAvail)))
    private val statusStream = Sinks.many()
        .multicast()
        .directBestEffort<UpstreamAvailability>()
    private val forksStream = Sinks.many()
        .multicast()
        .directBestEffort<Boolean>()
    private val forked = AtomicBoolean(false)

    init {
        if (id.length < 3 || !id.matches(Regex("[a-zA-Z][a-zA-Z0-9_-]+[a-zA-Z0-9]"))) {
            throw IllegalArgumentException("Invalid upstream id: $id")
        }

        if (options.disableValidation) {
            // if we specifically told that this upstream should be _always valid_ start with this state,
            // but note it could be updated later (ex. provided by gRPC upstream)
            this.setStatus(UpstreamAvailability.OK)
        }

        forkWatch.register(this)
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe {
                forked.set(it)
                forksStream.tryEmitNext(it)
            }
    }

    //
    // Can be used to temporarily disable the upstream until the specified time.
    // For example if it produces an error indicating there is too many requests.
    private val temporaryDisable = AtomicReference<Instant?>(null)

    val watchHttpCodes = Consumer<Int> { code ->
        if (CallQuorum.isConnectionUnavailable(code)) {
            val pause = Instant.now() + Duration.ofMinutes(1)
            temporaryDisable.updateAndGet { prev ->
                if (prev == null || prev < pause) {
                    pause
                } else {
                    prev
                }
            }
            statusStream.tryEmitNext(UpstreamAvailability.UNAVAILABLE)
        }
    }

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
        if (temporaryDisable.get()?.isAfter(Instant.now()) == true) {
            return UpstreamAvailability.UNAVAILABLE
        }
        if (forked.get()) {
            return UpstreamAvailability.IMMATURE
        }
        val value = status.get().status
        if (value == UpstreamAvailability.UNAVAILABLE || value == UpstreamAvailability.SYNCING) {
            return value
        }
        // if height is 0, then it's definitely on OK, probably just started with syncing
        if (this.getHead().getCurrentHeight() == 0L) {
            return UpstreamAvailability.SYNCING
        }
        return value
    }

    open fun setStatus(status: UpstreamAvailability) {
        this.status.updateAndGet { curr ->
            Status(curr.lag, status, statusByLag(curr.lag, status))
        }
        statusStream.tryEmitNext(this.status.get().status)
    }

    fun statusByLag(lag: Long, proposed: UpstreamAvailability): UpstreamAvailability {
        if (options.disableValidation == true) {
            // if we specifically told that this upstream should be _always valid_ then skip
            // the status calculation and trust the proposed value as is
            return proposed
        }
        return when {
            proposed == UpstreamAvailability.OK -> {
                // make sure it's actually usable
                when {
                    lag > 6 -> UpstreamAvailability.SYNCING
                    lag > 1 -> UpstreamAvailability.LAGGING
                    else -> proposed
                }
            }
            proposed == UpstreamAvailability.LAGGING && lag > 6 -> {
                // to large lag, mark as syncing
                UpstreamAvailability.SYNCING
            }
            else -> proposed
        }
    }

    val availabilityByForks: Flux<UpstreamAvailability>
        get() {
            return Flux.concat(
                Mono.just(forked.get()),
                Flux.from(forksStream.asFlux())
            ).map {
                if (it) UpstreamAvailability.IMMATURE else UpstreamAvailability.OK
            }.distinctUntilChanged()
        }

    val availabilityByStatus: Flux<UpstreamAvailability>
        get() {
            return Flux.concat(
                Mono.just(status.get().status),
                statusStream.asFlux().distinctUntilChanged()
            ).distinctUntilChanged()
        }

    override fun observeStatus(): Flux<UpstreamAvailability> {
        val byForks: Flux<UpstreamAvailability> = availabilityByForks
        val byHeight: Flux<UpstreamAvailability> = availabilityByStatus
        return MergedAvailability(byForks, byHeight).produce()
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
        return role
    }

    override fun getMethods(): CallMethods {
        return targets ?: throw IllegalStateException("Methods are not set")
    }

    override fun getBlockchain(): Chain {
        return chain
    }

    private val quorumByLabel = node?.let { QuorumForLabels(it) }
        ?: QuorumForLabels(QuorumForLabels.QuorumItem.empty())

    open fun getQuorumByLabel(): QuorumForLabels {
        return quorumByLabel
    }

    override fun start() {
        if (!this.forkWatch.isRunning) {
            this.forkWatch.start()
        }
    }

    override fun stop() {
        this.forkWatch.stop()
    }

    override fun isRunning(): Boolean {
        return this.forkWatch.isRunning
    }

    class Status(val lag: Long, val avail: UpstreamAvailability, val status: UpstreamAvailability)
}
