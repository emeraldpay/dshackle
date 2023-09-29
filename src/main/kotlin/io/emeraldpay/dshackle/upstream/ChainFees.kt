package io.emeraldpay.dshackle.upstream

import io.emeraldpay.api.proto.BlockchainOuterClass
import reactor.core.publisher.Mono

interface ChainFees {

    companion object {
        fun extractMode(req: BlockchainOuterClass.EstimateFeeRequest): Mode? {
            return when (req.mode!!) {
                BlockchainOuterClass.FeeEstimationMode.INVALID -> null
                BlockchainOuterClass.FeeEstimationMode.AVG_LAST -> Mode.AVG_LAST
                BlockchainOuterClass.FeeEstimationMode.AVG_T5 -> Mode.AVG_T5
                BlockchainOuterClass.FeeEstimationMode.AVG_T20 -> Mode.AVG_T20
                BlockchainOuterClass.FeeEstimationMode.AVG_T50 -> Mode.AVG_T50
                BlockchainOuterClass.FeeEstimationMode.MIN_ALWAYS -> Mode.MIN_ALWAYS
                BlockchainOuterClass.FeeEstimationMode.AVG_MIDDLE -> Mode.AVG_MIDDLE
                BlockchainOuterClass.FeeEstimationMode.AVG_TOP -> Mode.AVG_TOP
                BlockchainOuterClass.FeeEstimationMode.UNRECOGNIZED -> null
            }
        }
    }

    fun estimate(mode: Mode, blocks: Int): Mono<BlockchainOuterClass.EstimateFeeResponse>

    enum class Mode {
        AVG_LAST,
        AVG_T5,
        AVG_T20,
        AVG_T50,
        MIN_ALWAYS,
        AVG_MIDDLE,
        AVG_TOP,
    }
}
