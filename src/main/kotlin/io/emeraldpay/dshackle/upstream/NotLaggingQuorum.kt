package io.emeraldpay.dshackle.upstream

import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.json.BlockJson
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class NotLaggingQuorum(val maxLag: Long = 0): CallQuorum {

    private val result: AtomicReference<ByteArray> = AtomicReference()

    override fun init(head: Head<BlockJson<TransactionId>>) {
    }

    override fun isResolved(): Boolean {
        return result.get() != null
    }

    override fun record(response: ByteArray, upstream: Upstream) {
        val lagging = upstream.getLag() > maxLag
        if (!lagging) {
            result.set(response)
        }
    }

    override fun getResult(): ByteArray {
        return result.get()
    }
}