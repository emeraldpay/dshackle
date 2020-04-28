package io.emeraldpay.dshackle.rpc

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.grpc.Chain
import reactor.core.publisher.Flux

interface TrackTx {
    fun isSupported(chain: Chain): Boolean
    fun subscribe(request: BlockchainOuterClass.TxStatusRequest): Flux<BlockchainOuterClass.TxStatus>
}