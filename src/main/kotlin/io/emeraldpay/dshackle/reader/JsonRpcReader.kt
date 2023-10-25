package io.emeraldpay.dshackle.reader

import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse

typealias JsonRpcReader = Reader<JsonRpcRequest, JsonRpcResponse>

interface JsonRpcHttpReader : JsonRpcReader {
    fun onStop()
}
