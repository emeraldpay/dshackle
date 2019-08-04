package io.emeraldpay.dshackle.upstream

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.grpc.Chain
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import reactor.core.publisher.Flux
import reactor.core.publisher.TopicProcessor
import java.util.*
import kotlin.collections.LinkedHashSet

@Repository
class AvailableChains(
        @Autowired private val objectMapper: ObjectMapper
) {

    private val all = LinkedHashSet<Chain>()
    private val bus = TopicProcessor.create<Chain>()
    private val callTargets = HashMap<Chain, EthereumTargets>()

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

    fun targetFor(chain: Chain): EthereumTargets {
        var current = callTargets[chain]
        if (current == null) {
            current = EthereumTargets(objectMapper, chain)
            callTargets[chain] = current
        }
        return current
    }
}