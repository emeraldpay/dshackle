package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.reader.DshackleRpcReader
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleRequest
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleResponse
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import java.util.function.Predicate

open class MethodSpecificReader : DshackleRpcReader {

    companion object {
        private val log = LoggerFactory.getLogger(MethodSpecificReader::class.java)
    }

    private val specific = mutableMapOf<String, MutableList<Delegate>>()

    fun register(method: String, reader: DshackleRpcReader) {
        this.register(method, { true }, reader)
    }

    fun register(method: String, params: Predicate<List<Any?>>, reader: DshackleRpcReader) {
        val current: MutableList<Delegate> = if (specific.containsKey(method)) {
            specific[method]!!
        } else {
            val created = mutableListOf<Delegate>()
            specific[method] = created
            created
        }
        current.add(Delegate(params, reader))
    }

    override fun read(key: DshackleRequest): Mono<DshackleResponse> {
        return specific[key.method]?.find {
            it.params.test(key.params)
        }?.reader?.read(key) ?: Mono.empty()
    }

    data class Delegate(
        val params: Predicate<List<Any?>>,
        val reader: DshackleRpcReader,
    )
}
