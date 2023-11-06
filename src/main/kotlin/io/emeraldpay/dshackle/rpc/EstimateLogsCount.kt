package io.emeraldpay.dshackle.rpc

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.upstream.MultistreamHolder
import io.grpc.Status
import io.grpc.StatusException
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono


@Service
class EstimateLogsCount(
    @Autowired private val multistreamHolder: MultistreamHolder,
) {

    companion object {
        private val log = LoggerFactory.getLogger(EstimateLogsCount::class.java)
    }

    fun estimateLogsCount(req: BlockchainOuterClass.EstimateLogsCountRequest): Mono<BlockchainOuterClass.EstimateLogsCountResponse> {
        val chain = Chain.byId(req.chainValue)
        val up = multistreamHolder.getUpstream(chain) ?: return Mono.error(
            StatusException(
                Status.UNAVAILABLE.withDescription("BLOCKCHAIN UNAVAILABLE: ${req.chainValue}"),
            ),
        )

        return up.estimateLogsCount(req)
    }
}
