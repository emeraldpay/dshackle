package io.emeraldpay.dshackle.upstream

enum class UpstreamAvailability(val grpcId: Int) {

    /**
     * Active fully synchronized node
     */
    OK(1),
    /**
     * Good node, but is still synchronizing a latest block
     */
    LAGGING(2),
    /**
     * May be good, but node doesn't have enough peers to be sure it's on corrected chain
     */
    IMMATURE(3),
    /**
     * Node is doing it's initial synchronization, is behind by at least several blocks
     */
    SYNCING(4),
    /**
     * Unavailable node
     */
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