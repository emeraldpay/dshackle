package io.emeraldpay.dshackle.upstream

import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.json.BlockJson
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class NotLaggingQuorum(val maxLag: Long = 0): CallQuorum {

    private var head: Flux<BlockJson<TransactionId>> = Flux.empty<BlockJson<TransactionId>>()
    private val lock = ReentrantLock()
    private var resolved = false
    private var result: ByteArray? = null

    override fun init(head: Head<BlockJson<TransactionId>>) {
        this.head = head.getFlux()
    }

    override fun isResolved(): Boolean {
        return resolved && result != null
    }

    override fun record(response: ByteArray, upstream: Upstream) {
        Mono.from(head)
                .zipWith(upstream.getHead().getHead())
                .map {
                    val top = it.t1
                    val current = it.t2
                    return@map (top.number - current.number) < maxLag
                }.subscribe { fresh ->
                    if (fresh) {
                        lock.withLock {
                            result = response
                            resolved = true
                        }
                    }
                }
    }

    override fun getResult(): ByteArray {
        return result!!
    }
}