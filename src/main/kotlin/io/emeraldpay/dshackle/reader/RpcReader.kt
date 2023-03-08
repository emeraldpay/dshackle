package io.emeraldpay.dshackle.reader

import io.emeraldpay.dshackle.upstream.rpcclient.DshackleRequest
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleResponse
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse

typealias DshackleRpcReader = Reader<DshackleRequest, DshackleResponse>
typealias StandardRpcReader = Reader<JsonRpcRequest, JsonRpcResponse>
