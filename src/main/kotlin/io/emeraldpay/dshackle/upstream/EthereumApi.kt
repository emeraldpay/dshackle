package io.emeraldpay.dshackle.upstream

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.hex.HexQuantity
import io.infinitape.etherjar.rpc.*
import io.infinitape.etherjar.rpc.json.ResponseJson
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import java.time.Duration

open class EthereumApi(
        val rpcClient: RpcClient,
        private val objectMapper: ObjectMapper,
        private val chain: Chain,
        val targets: CallMethods
) {

    private val jacksonRpcConverter = JacksonRpcConverter(objectMapper)
    var upstream: Upstream? = null

    private val timeout = Duration.ofSeconds(5)
    private val log = LoggerFactory.getLogger(EthereumApi::class.java)
    var ws: EthereumWs? = null
        set(value) {
            field = value
        }

    open fun <JS, RS> executeAndConvert(rpcCall: RpcCall<JS, RS>): Mono<RS> {
        val convertToJS = java.util.function.Function<ByteArray, Mono<JS>> { resp ->
            val jsonValue: JS? = jacksonRpcConverter.fromJson(resp.inputStream(), rpcCall.jsonType, Int::class.java)
            if (jsonValue == null) Mono.empty<JS>()
            else Mono.just(jsonValue)
        }
        return execute(0, rpcCall.method, rpcCall.params as List<Any>)
                .flatMap(convertToJS)
                .map(rpcCall.converter::apply)
    }

    open fun execute(id: Int, method: String, params: List<Any>): Mono<ByteArray> {
        val result: Mono<Any> = if (targets.isHardcoded(method)) {
            Mono.just(method)
                    .map { targets.hardcoded(it) }
        } else if (targets.isAllowed(method)) {
            callUpstream(method, params)
        } else {
            Mono.error(RpcException(-32601, "Method not allowed or not found"))
        }
        return result
                .doOnError { t ->
                    log.warn("Upstream error: ${t.message} for ${method} on $chain")
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

    private fun callUpstream(method: String, params: List<Any>): Mono<Any> {
        if (ws != null && method == "eth_blockNumber") {
            val head = ws!!.getHead()
            if (head != null) {
                return Mono.just(HexQuantity.from(head.number).toHex())
            }
        }
        return Mono.fromCompletionStage(
                rpcClient.execute(RpcCall.create(method, Any::class.java, params))
        ).timeout(timeout, Mono.error(RpcException(-32603, "Upstream timeout")))
    }

}