package io.emeraldpay.dshackle.upstream.forkchoice

import com.google.common.cache.CacheBuilder
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicReference

class PriorityForkChoice : ForkChoice {
    private val head = AtomicReference<BlockContainer>(null)
    private val seenBlocks = CacheBuilder.newBuilder()
        .maximumSize(10)
        .build<BlockId, Boolean>()

    companion object {
        private val log = LoggerFactory.getLogger(PriorityForkChoice::class.java)
    }

    override fun getHead(): BlockContainer? {
        return head.get()
    }

    override fun filter(block: BlockContainer): Boolean {
        val curr = head.get()
        return seenBlocks.getIfPresent(block.hash) == null && block.height > (curr?.height ?: 0)
    }

    override fun choose(block: BlockContainer): ForkChoice.ChoiceResult {
        head.get()?.let {
            log.trace("Candidate for priority forkchoice (${block.height}, ${block.nodeRating}), current is (${it.height},${it.nodeRating})")
        }
        val nwhead = head.updateAndGet { curr ->
            if (!filter(block)) {
                log.trace("Preparing to deny block ${block.height}")
                curr
            } else {
                log.trace("Preparing to accept block ${block.height}")
                seenBlocks.put(block.hash, true)
                block
            }
        }
        if (nwhead.hash == block.hash) {
            log.trace("Accepted block ${block.height}")
            return ForkChoice.ChoiceResult.Updated(nwhead)
        }
        log.trace("Denied block ${block.height}")
        return ForkChoice.ChoiceResult.Same(nwhead)
    }
}
