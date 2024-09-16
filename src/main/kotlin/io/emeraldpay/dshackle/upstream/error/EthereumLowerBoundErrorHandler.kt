package io.emeraldpay.dshackle.upstream.error

import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundType
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import org.slf4j.LoggerFactory

abstract class EthereumLowerBoundErrorHandler : ErrorHandler {
    protected val log = LoggerFactory.getLogger(this::class.java)

    override fun handle(upstream: Upstream, request: ChainRequest, errorMessage: String?) {
        try {
            if (canHandle(request, errorMessage)) {
                parseTagParam(request, tagIndex(request.method))?.let {
                    upstream.updateLowerBound(it, type())
                }
            }
        } catch (e: RuntimeException) {
            log.warn("Couldn't update the {} lower bound of {}, reason - {}", type(), upstream.getId(), e.message)
        }
    }

    protected abstract fun tagIndex(method: String): Int

    protected abstract fun type(): LowerBoundType

    private fun parseTagParam(request: ChainRequest, tagIndex: Int): Long? {
        if (tagIndex != -1 && request.params is ListParams) {
            val params = request.params.list
            if (params.size >= tagIndex) {
                val tag = params[tagIndex]
                if (tag is String && tag.startsWith("0x")) {
                    return tag.substring(2).toLong(16)
                }
            }
        }
        return null
    }
}
