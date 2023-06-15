package io.emeraldpay.dshackle.rpc

import io.emeraldpay.api.Chain
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.dshackle.upstream.ChainFees
import io.emeraldpay.dshackle.upstream.MultistreamHolder
import io.grpc.Status
import io.grpc.StatusException
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

@Service
class EstimateFee(
    @Autowired private val multistreamHolder: MultistreamHolder
) {

    companion object {
        private val log = LoggerFactory.getLogger(EstimateFee::class.java)
    }

    fun estimateFee(req: BlockchainOuterClass.EstimateFeeRequest): Mono<BlockchainOuterClass.EstimateFeeResponse> {
        val chain = Chain.byId(req.chainValue)
        val up = multistreamHolder.getUpstream(chain) ?: return Mono.error(
            StatusException(
                Status.UNAVAILABLE.withDescription("BLOCKCHAIN UNAVAILABLE: ${req.chainValue}")
            )
        )
        val mode = ChainFees.extractMode(req) ?: return Mono.error(
            StatusException(
                Status.UNAVAILABLE.withDescription("UNSUPPORTED MODE: ${req.mode.number}")
            )
        )
        return up.getFeeEstimation()
            .estimate(mode, req.blocks)
    }
}
