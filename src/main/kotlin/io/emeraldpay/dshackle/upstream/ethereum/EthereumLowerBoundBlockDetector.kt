package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.upstream.RecursiveLowerBoundBlockDetector
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.dshackle.upstream.toHex
import reactor.core.publisher.Mono

class EthereumLowerBoundBlockDetector(
    chain: Chain,
    private val upstream: Upstream,
) : RecursiveLowerBoundBlockDetector(chain, upstream) {

    override fun hasState(blockNumber: Long): Mono<Boolean> {
        return upstream.getIngressReader().read(
            JsonRpcRequest(
                "eth_getBalance",
                listOf("0x756F45E3FA69347A9A973A725E3C98bC4db0b5a0", blockNumber.toHex()),
            ),
        )
            .flatMap(JsonRpcResponse::requireResult)
            .map { true }
            .onErrorReturn(false)
    }
}
