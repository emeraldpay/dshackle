package io.emeraldpay.dshackle.upstream

enum class UpstreamAvailability(val grpcId: Int) {

    OK(1),
    IMMATURE(2),
    SYNCING(3),
    LAGGING(4),
    UNAVAILABLE(5);

    companion object {
        fun fromGrpc(id: Int?): UpstreamAvailability {
            if (id == null) {
                return UNAVAILABLE
            }
            return UpstreamAvailability.values().find {
                it.grpcId == id
            } ?: UNAVAILABLE
        }
    }
}