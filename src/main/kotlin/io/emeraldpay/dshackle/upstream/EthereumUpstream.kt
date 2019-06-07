package io.emeraldpay.dshackle.upstream

import com.fasterxml.jackson.databind.ObjectMapper
import io.infinitape.etherjar.rpc.json.ResponseJson
import io.infinitape.etherjar.rpc.transport.RpcTransport
import reactor.core.publisher.Mono

class EthereumUpstream(
        private val rpcTransport: RpcTransport,
        private val objectMapper: ObjectMapper
) {

    fun execute(id: Int, method: String, params: List<Any>): Mono<ByteArray> {
        return Mono
                .fromCompletionStage(rpcTransport.execute(method, params, Any::class.java))
                .map {
                    val resp = ResponseJson<Any, Int>()
                    resp.id = id
                    resp.result = it
                    objectMapper.writer().writeValueAsBytes(resp)
                }
    }

}