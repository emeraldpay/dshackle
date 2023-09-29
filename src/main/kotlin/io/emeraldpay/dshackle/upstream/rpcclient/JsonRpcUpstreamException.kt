package io.emeraldpay.dshackle.upstream.rpcclient

class JsonRpcUpstreamException(
    id: JsonRpcResponse.Id,
    error: JsonRpcError,
) : JsonRpcException(id, error, null, false)
