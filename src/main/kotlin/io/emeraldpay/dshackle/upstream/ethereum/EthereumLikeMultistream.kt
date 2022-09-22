package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.upstream.Upstream

interface EthereumLikeMultistream : Upstream {
    fun getReader(): EthereumReader
    fun getSubscribe(): EthereumSubscribe
}
