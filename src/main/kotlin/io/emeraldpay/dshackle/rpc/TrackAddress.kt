package io.emeraldpay.dshackle.rpc

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.grpc.Chain
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

/**
 * Base interface to tracking balance on a single blockchain
 */
interface TrackAddress {

    fun isSupported(chain: Chain): Boolean
    fun getBalance(requestMono: BlockchainOuterClass.BalanceRequest): Flux<BlockchainOuterClass.AddressBalance>
    fun subscribe(requestMono: BlockchainOuterClass.BalanceRequest): Flux<BlockchainOuterClass.AddressBalance>

}