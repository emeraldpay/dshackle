package io.emeraldpay.dshackle.upstream

import com.fasterxml.jackson.databind.ObjectMapper
import io.infinitape.etherjar.rpc.RpcCall
import io.infinitape.etherjar.rpc.RpcClient
import io.infinitape.etherjar.rpc.RpcException
import io.infinitape.etherjar.rpc.RpcResponseError
import io.infinitape.etherjar.rpc.json.ResponseJson
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import java.time.Duration

class EthereumUpstream(
        private val rpcClient: RpcClient,
        private val objectMapper: ObjectMapper
) {

    private val timeout = Duration.ofSeconds(5)
    private val log = LoggerFactory.getLogger(EthereumUpstream::class.java)

    fun execute(id: Int, method: String, params: List<Any>): Mono<ByteArray> {
        return Mono
                .fromCompletionStage(
                        rpcClient.execute(RpcCall.create(method, Any::class.java, params))
                )
                .timeout(timeout)
                .doOnError { t ->
                    log.warn("Upstream error: ${t.message}")
                }
                .map {
                    val resp = ResponseJson<Any, Int>()
                    resp.id = id
                    resp.result = it
                    objectMapper.writer().writeValueAsBytes(resp)
                }
                .onErrorMap { t ->
                    if (RpcException::class.java.isAssignableFrom(t.javaClass)) {
                        t
                    } else {
                        log.warn("Convert to RPC error. Exception: ${t.message}")
                        RpcException(-32020, "Error reading from upstream", null, t)
                    }
                }
                .onErrorResume(RpcException::class.java) { t ->
                    val resp = ResponseJson<Any, Int>()
                    resp.id = id
                    resp.error = t.error
                    Mono.just(objectMapper.writer().writeValueAsBytes(resp))
                }
    }

}