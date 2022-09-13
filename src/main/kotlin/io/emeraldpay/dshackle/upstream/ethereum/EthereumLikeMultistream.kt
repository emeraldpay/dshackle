package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.Upstream

interface EthereumLikeMultistream : Upstream {
    fun getReader(): EthereumReader
    fun getSubscribe(): EthereumSubscribe

    fun getHead(mather: Selector.Matcher): Head
}
