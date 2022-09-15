package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.Upstream
import reactor.core.publisher.Flux

interface EthereumLikeMultistream : Upstream {
    fun getReader(): EthereumReader
    fun getSubscribe(): EthereumSubscribe

    fun getHead(mather: Selector.Matcher): Head

    fun tryProxy(mather: Selector.Matcher, request: BlockchainOuterClass.NativeSubscribeRequest): Flux<out Any>?
}
