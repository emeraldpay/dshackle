package io.emeraldpay.dshackle.upstream.bitcoin

import com.fasterxml.jackson.databind.JavaType
import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.upstream.UpstreamApi
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.infinitape.etherjar.rpc.RpcException
import io.infinitape.etherjar.rpc.RpcResponseError
import io.infinitape.etherjar.rpc.json.FullResponseJson
import io.infinitape.etherjar.rpc.json.RequestJson
import io.infinitape.etherjar.rpc.json.ResponseJson
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono

open class BitcoinApi(
        val bitcoinRpcClient: BitcoinRpcClient,
        val objectMapper: ObjectMapper,
        val targets: CallMethods
) : UpstreamApi {

    companion object {
        private val log = LoggerFactory.getLogger(BitcoinApi::class.java)
    }

    open override fun execute(id: Int, method: String, params: List<Any>): Mono<ByteArray> {
        //TODO it's almost the same code as for DirectEthereumApi; refactor
        val result: Mono<out Any> = when {
            targets.isHardcoded(method) -> Mono.just(method).map { targets.executeHardcoded(it) }
            targets.isAllowed(method) -> executeAndResult(id, method, params, Object::class.java)
            else -> Mono.error(RpcException(-32601, "Method not allowed or not found"))
        }
        return processResult(id, method, result)
    }

    public fun processResult(id: Int, method: String, result: Mono<out Any>): Mono<ByteArray> {
        //TODO it's the same code as for DirectEthereumApi; refactor
        return result
                .doOnError { t ->
                    log.warn("Upstream error: [${t.message}] for $method")
                }
                .map {
                    val resp = ResponseJson<Any, Int>()
                    resp.id = id
                    resp.result = it
                    resp
                }
                .switchIfEmpty(
                        Mono.fromCallable {
                            val resp = ResponseJson<Any, Int>()
                            resp.id = id
                            resp.result = null
                            resp
                        }
                )
                .map {
                    objectMapper.writer().writeValueAsBytes(it)
                }
                .onErrorResume(StatusRuntimeException::class.java) { t ->
                    if (t.status.code == Status.Code.CANCELLED) {
                        Mono.empty<ByteArray>()
                    } else {
                        Mono.error(RpcException(RpcResponseError.CODE_UPSTREAM_CONNECTION_ERROR, "gRPC error ${t.status}"))
                    }
                }
                .onErrorMap { t ->
                    if (RpcException::class.java.isAssignableFrom(t.javaClass)) {
                        t
                    } else {
                        log.warn("Convert to RPC error. Exception ${t.javaClass}:${t.message}", t)
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

    open fun <T> executeAndResult(id: Int, method: String, params: List<Any>, resultType: Class<T>): Mono<T> {
        val rpc = RequestJson<Int>(method, params, id)
        return Mono.just(rpc)
                .map(objectMapper::writeValueAsBytes)
                .flatMap(bitcoinRpcClient::execute)
                .flatMap { json ->
                    val type: JavaType = objectMapper.typeFactory.constructParametricType(FullResponseJson::class.java, resultType, Int::class.java)
                    val resp = objectMapper.readerFor(type).readValue<FullResponseJson<T, Int>>(json)
                    if (resp.hasError()) {
                        Mono.error(resp.error.asException())
                    } else {
                        Mono.just(resp.result)
                    }
                }
    }

    open fun getBlock(hash: String): Mono<Map<String, Any>> {
        return executeAndResult(0, "getblock", listOf(hash), Map::class.java) as Mono<Map<String, Any>>
    }

    open fun getTx(txid: String): Mono<Map<String, Any>> {
        return executeAndResult(0, "getrawtransaction", listOf(txid, true), Map::class.java) as Mono<Map<String, Any>>
    }

    open fun getMempool(): Mono<List<String>> {
        return executeAndResult(0, "getrawmempool", emptyList(), List::class.java) as Mono<List<String>>
    }
}