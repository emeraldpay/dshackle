package io.emeraldpay.dshackle.upstream.error

import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.ethereum.EthereumLowerBoundStateDetector.Companion.stateErrors
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundType

object EthereumTraceLowerBoundErrorHandler : EthereumLowerBoundErrorHandler() {
    private val zeroTagIndexMethods = setOf(
        "trace_block",
        "arbtrace_block",
        "debug_traceBlockByNumber",
    )
    private val firstTagIndexMethods = setOf(
        "trace_callMany",
        "arbtrace_callMany",
        "debug_traceCall",
    )
    private val secondTagIndexMethods = setOf(
        "trace_call",
        "arbtrace_call",
    )

    private val applicableMethods = zeroTagIndexMethods + firstTagIndexMethods + secondTagIndexMethods

    private val errors = stateErrors
        .plus(
            setOf(
                "historical state not available",
            ),
        )
    private val errorRegexp = Regex("block .* not found")

    override fun canHandle(request: ChainRequest, errorMessage: String?): Boolean {
        return (errors.any { errorMessage?.contains(it) ?: false } || (errorMessage?.matches(errorRegexp) ?: false)) &&
            applicableMethods.contains(request.method)
    }

    override fun tagIndex(method: String): Int {
        return if (firstTagIndexMethods.contains(method)) {
            1
        } else if (secondTagIndexMethods.contains(method)) {
            2
        } else if (zeroTagIndexMethods.contains(method)) {
            0
        } else {
            -1
        }
    }

    override fun type(): LowerBoundType = LowerBoundType.TRACE
}
