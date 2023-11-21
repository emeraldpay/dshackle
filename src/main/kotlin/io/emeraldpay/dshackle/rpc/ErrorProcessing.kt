package io.emeraldpay.dshackle.rpc

import org.springframework.stereotype.Component

interface ErrorProcessor {

    fun matches(result: NativeCall.CallResult): Boolean

    fun errorProcess(error: NativeCall.CallError): NativeCall.CallError
}

@Component
open class ErrorCorrector(
    private val processors: List<ErrorProcessor>,
) {

    fun correctError(result: NativeCall.CallResult): NativeCall.CallError {
        if (!result.isError()) {
            throw IllegalStateException("No error to correct")
        }
        return processors
            .firstOrNull { it.matches(result) }
            ?.errorProcess(result.error!!)
            ?: result.error!!
    }
}

@Component
class NethermindEthCallRevertedErrorProcessor : ErrorProcessor {
    override fun matches(result: NativeCall.CallResult): Boolean {
        return result.ctx?.payload?.method == "eth_call" && (result.error?.data?.startsWith("Reverted") ?: false)
    }

    override fun errorProcess(error: NativeCall.CallError): NativeCall.CallError {
        return NativeCall.CallError(
            3,
            error.message,
            error.upstreamError,
            error.data?.removePrefix("Reverted "),
            error.upstreamId,
        )
    }
}
