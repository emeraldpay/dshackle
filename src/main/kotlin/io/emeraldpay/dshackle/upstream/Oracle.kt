package io.emeraldpay.dshackle.upstream

import io.emeraldpay.api.proto.BlockchainOuterClass
import org.drpc.logsoracle.*;

class Oracle(
    var dir: String,
    var ramLimit: Long = 0,
) {
    val db = LogsOracle(dir, ramLimit)

    fun estimate(request: BlockchainOuterClass.EstimateLogsCountRequest): BlockchainOuterClass.EstimateLogsCountResponse {
        return BlockchainOuterClass.EstimateLogsCountResponse.newBuilder()
            .setSucceed(true)
            .setCount(42)
            .build()

        // return db.query();
    }
}
