package io.emeraldpay.dshackle.upstream.forkchoice

import com.google.common.cache.CacheBuilder
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicReference

class NoChoiceWithPriorityForkChoice(
    private val nodeRating: Int,
    private val upstreamId: String
) : ForkChoice {
    private val head = AtomicReference<BlockContainer>(null)
    private val seenBlocks = CacheBuilder.newBuilder()
        .maximumSize(100)
        .build<BlockId, Boolean>()

    companion object {
        private val log = LoggerFactory.getLogger(NoChoiceWithPriorityForkChoice::class.java)
    }
    override fun getHead(): BlockContainer? {
        return head.get()
    }

    override fun filter(block: BlockContainer): Boolean {
        return seenBlocks.getIfPresent(block.hash) == null
    }

    override fun choose(block: BlockContainer): ForkChoice.ChoiceResult {
        log.debug("Adding priority to $upstreamId block ${block.height}")
        val nwhead = head.updateAndGet { curr ->
            if (!filter(block)) {
                log.debug("Already seen block ${block.height} from $upstreamId")
                curr
            } else {
                seenBlocks.put(block.hash, true)
                block.copyWithRating(nodeRating)
            }
        }
        if (nwhead.hash == block.hash) {
            log.debug("Accepted block ${block.height} from $upstreamId with $nodeRating")
            return ForkChoice.ChoiceResult.Updated(nwhead)
        }
        log.debug("Declined block ${block.height} from $upstreamId with $nodeRating")
        return ForkChoice.ChoiceResult.Same(nwhead)
    }
}
