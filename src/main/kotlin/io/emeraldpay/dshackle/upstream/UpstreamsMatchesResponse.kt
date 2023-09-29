package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.upstream.MatchesResponse.HeightResponse
import io.emeraldpay.dshackle.upstream.MatchesResponse.MultiResponse
import kotlin.math.min

class UpstreamsMatchesResponse {

    companion object {
        private val possibleNullReturnMethods = listOf(
            "eth_getTransactionByHash",
            "eth_getTransactionReceipt",
            "eth_getBlockByHash",
            "eth_getBlockByNumber",
            "eth_getTransactionByBlockHashAndIndex",
            "eth_getTransactionByBlockNumberAndIndex",
            "eth_getUncleByBlockHashAndIndex",
            "eth_getUncleByBlockNumberAndIndex",
        )
    }

    private val responses = LinkedHashSet<UpstreamNotMatchedResponse>()

    fun addUpstreamMatchesResponse(upstreamId: String, response: MatchesResponse) {
        if (!response.matched()) {
            responses.add(UpstreamNotMatchedResponse(upstreamId, response))
        }
    }

    fun getFullCause(): String? =
        if (responses.isEmpty()) {
            null
        } else {
            responses
                .joinToString("; ") { "${it.upstreamId} - ${it.matchesResponse.getCause()}" }
                .run {
                    substring(0, min(200, this.length))
                }
        }

    fun getCause(method: String): NotMatchesCause? {
        if (responses.isEmpty()) {
            return null
        }
        val commonMatchesResponse = hasCommonMatchesResponse()
        return if (commonMatchesResponse is HeightResponse && possibleNullReturnMethods.contains(method)) {
            NotMatchesCause(true)
        } else if (commonMatchesResponse != null && commonMatchesResponse !is HeightResponse) {
            NotMatchesCause(false, commonMatchesResponse.getCause())
        } else {
            null
        }
    }

    private fun hasCommonMatchesResponse(): MatchesResponse? {
        val matchesResponses = responses.map { it.matchesResponses }
        val commonResponses = mutableListOf<MatchesResponse>()
        for (matchesResponse in matchesResponses[0]) {
            if (matchesResponses.all { it.any { resp -> matchesResponse.javaClass == resp.javaClass } }) {
                commonResponses.add(matchesResponse)
            }
        }
        if (commonResponses.isEmpty()) {
            return null
        }
        val heightResponse = commonResponses.find { it is HeightResponse }
        return heightResponse ?: commonResponses[0]
    }

    data class NotMatchesCause(
        val shouldReturnNull: Boolean,
        val cause: String? = null,
    )

    private class UpstreamNotMatchedResponse(
        val upstreamId: String,
        val matchesResponse: MatchesResponse,
    ) {
        val matchesResponses: Set<MatchesResponse> = when (matchesResponse) {
            is MultiResponse -> matchesResponse.allResponses
            else -> setOf(matchesResponse)
        }

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as UpstreamNotMatchedResponse

            if (upstreamId != other.upstreamId) return false

            return true
        }

        override fun hashCode(): Int {
            return upstreamId.hashCode()
        }
    }
}
