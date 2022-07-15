package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.grpc.Chain

interface HttpFactory {
    fun create(id: String?, chain: Chain): Reader<JsonRpcRequest, JsonRpcResponse>
}