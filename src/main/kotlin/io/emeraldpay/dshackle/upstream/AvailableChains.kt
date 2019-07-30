package io.emeraldpay.dshackle.upstream

import io.emeraldpay.grpc.Chain
import org.springframework.stereotype.Repository
import reactor.core.publisher.Flux
import reactor.core.publisher.TopicProcessor
import java.util.*
import kotlin.collections.LinkedHashSet

@Repository
class AvailableChains {

    private val all = LinkedHashSet<Chain>()
    private val bus = TopicProcessor.create<Chain>()

    fun add(chain: Chain) {
        all.add(chain)
        bus.onNext(chain)
    }

    fun observe(): Flux<Chain> {
        return Flux.from(bus)
    }

    fun supports(chain: Chain): Boolean {
        return all.contains(chain)
    }

    fun getAll(): Set<Chain> {
        return Collections.unmodifiableSet(all)
    }
}