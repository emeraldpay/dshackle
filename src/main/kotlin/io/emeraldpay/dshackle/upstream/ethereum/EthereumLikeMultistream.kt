package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.dshackle.upstream.HasEgressSubscription
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.Upstream
import reactor.core.publisher.Flux

interface EthereumLikeMultistream : Upstream, HasEgressSubscription {
    fun getReader(): EthereumCachingReader

    fun getHead(mather: Selector.Matcher): Head

    fun getEnrichedHead(mather: Selector.Matcher): Head

    /**
     * Tries to proxy the native subscribe request to the managed upstreams if
     * - any of them matches the matcher criteria
     * - all of matching above are gRPC ones
     * in this case the upstream dshackle instances can sign the results and they will just proxied as is with original signs
     * Otherwise return null
     */
    fun tryProxy(matcher: Selector.Matcher, request: BlockchainOuterClass.NativeSubscribeRequest): Flux<out Any>?
}
