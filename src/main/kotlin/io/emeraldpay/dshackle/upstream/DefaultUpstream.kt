package io.emeraldpay.dshackle.upstream

import reactor.core.publisher.Flux
import reactor.core.publisher.TopicProcessor
import java.util.concurrent.atomic.AtomicReference

abstract class DefaultUpstream(
        lag: Long,
        avail: UpstreamAvailability
) : Upstream {

    constructor() : this(Long.MAX_VALUE, UpstreamAvailability.UNAVAILABLE)

    private val status = AtomicReference(Status(lag, avail, statusByLag(lag, avail)))
    private val statusStream: TopicProcessor<UpstreamAvailability> = TopicProcessor.create()

    override fun getStatus(): UpstreamAvailability {
        return status.get().status
    }

    fun setStatus(avail: UpstreamAvailability) {
        status.updateAndGet { curr ->
            Status(curr.lag, avail, statusByLag(curr.lag, avail))
        }
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
        return Flux.from(statusStream)
    }

    override fun setLag(lag: Long) {
        if (lag < 0) {
            setLag(0)
        } else {
            status.updateAndGet { curr ->
                Status(lag, curr.avail, statusByLag(lag, curr.avail))
            }
        }
    }

    override fun getLag(): Long {
        return this.status.get().lag
    }

    class Status(val lag: Long, val avail: UpstreamAvailability, val status: UpstreamAvailability)
}