package io.emeraldpay.dshackle.upstream

sealed class MatchesResponse {

    fun matched(): Boolean {
        return when (this) {
            is Success -> true
            else -> false
        }
    }

    fun getCause(): String? =
        when (this) {
            is LabelResponse -> "No label `${this.name}` with values ${this.values}"
            AvailabilityResponse -> "Upstream is not available"
            is CapabilityResponse -> "Upstream does not have capability ${this.capability}"
            is ExistsResponse -> "Label ${this.name} does not exist"
            GrpcResponse -> "Upstream is not grpc"
            is HeightResponse -> "Upstream height ${this.currentHeight} is less than ${this.height}"
            is SlotHeightResponse -> "Upstream slot height ${this.currentSlotHeight} is less than ${this.slot}"
            is MethodResponse -> "Method ${this.method} is not supported"
            is MultiResponse ->
                this.allResponses
                    .filter { it !is Success }
                    .joinToString("; ") { it.getCause()!! }
            is NotMatchedResponse -> "Not matched - ${response.getCause()}"
            is SameNodeResponse -> "Upstream does not have hash ${this.upstreamHash}"
            else -> null
        }

    object Success : MatchesResponse()

    data class LabelResponse(
        val name: String,
        val values: Collection<String>,
    ) : MatchesResponse()

    data class NotMatchedResponse(
        val response: MatchesResponse,
    ) : MatchesResponse()

    data class MultiResponse(
        private val responses: Set<MatchesResponse>,
    ) : MatchesResponse() {
        val allResponses = mutableSetOf<MatchesResponse>()

        init {
            responses.forEach {
                if (it is MultiResponse) {
                    it.allResponses.forEach { resp ->
                        allResponses.add(resp)
                    }
                } else {
                    allResponses.add(it)
                }
            }
        }
    }

    data class MethodResponse(
        val method: String,
    ) : MatchesResponse()

    data class ExistsResponse(
        val name: String,
    ) : MatchesResponse()

    data class CapabilityResponse(
        val capability: Capability,
    ) : MatchesResponse()

    object GrpcResponse : MatchesResponse()

    data class HeightResponse(
        val height: Long,
        val currentHeight: Long,
    ) : MatchesResponse()

    data class SlotHeightResponse(
        val slot: Long,
        val currentSlotHeight: Long,
    ) : MatchesResponse()

    data class SameNodeResponse(
        val upstreamHash: Short,
    ) : MatchesResponse()

    object AvailabilityResponse : MatchesResponse()
}
