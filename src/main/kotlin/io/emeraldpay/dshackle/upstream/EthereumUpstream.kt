package io.emeraldpay.dshackle.upstream

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.rpc.RpcCall
import io.infinitape.etherjar.rpc.RpcClient
import io.infinitape.etherjar.rpc.RpcException
import io.infinitape.etherjar.rpc.json.ResponseJson
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import java.time.Duration
import java.util.*

class EthereumUpstream(
        private val rpcClient: RpcClient,
        private val objectMapper: ObjectMapper,
        private val chain: Chain
) {

    private val timeout = Duration.ofSeconds(5)
    private val log = LoggerFactory.getLogger(EthereumUpstream::class.java)

    private val allowedMethods = listOf(
            "eth_gasPrice",
            "eth_blockNumber",
            "eth_getBalance",
            "eth_getStorageAt",
            "eth_getTransactionCount",
            "eth_getBlockTransactionCountByHash",
            "eth_getBlockTransactionCountByNumber",
            "eth_getUncleCountByBlockHash",
            "eth_getUncleCountByBlockNumber",
            "eth_getCode",
            "eth_sendRawTransaction",
            "eth_call",
            "eth_estimateGas",
            "eth_getBlockByHash",
            "eth_getBlockByNumber",
            "eth_getTransactionByHash",
            "eth_getTransactionByBlockHashAndIndex",
            "eth_getTransactionByBlockNumberAndIndex",
            "eth_getTransactionReceipt",
            "eth_getUncleByBlockHashAndIndex",
            "eth_getUncleByBlockNumberAndIndex"
    )

    private val hardcodedMethods = listOf(
            "net_version",
            "net_peerCount",
            "net_listening",
            "web3_clientVersion",
            "eth_protocolVersion",
            "eth_syncing",
            "eth_coinbase",
            "eth_mining",
            "eth_hashrate",
            "eth_accounts"
    )

    fun execute(id: Int, method: String, params: List<Any>): Mono<ByteArray> {
        val result: Mono<Any> = if (hardcodedMethods.contains(method)) {
            Mono.just(method)
                .map{ hardcoded(it) }
        } else if (allowedMethods.contains(method)) {
            Mono.fromCompletionStage(
                        rpcClient.execute(RpcCall.create(method, Any::class.java, params))
                )
                .timeout(timeout)
        } else {
            Mono.error(RpcException(-32601, "Method not allowed or not found"))
        }
        return result
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

    fun hardcoded(method: String): Any {
        if ("net_version" == method) {
            if (Chain.ETHEREUM == chain) {
                return "1"
            }
            if (Chain.ETHEREUM_CLASSIC == chain) {
                return "1"
            }
            if (Chain.MORDEN == chain) {
                return "2"
            }
            throw RpcException(-32602, "Invalid chain")
        }
        if ("net_peerCount" == method) {
            return "0x2a"
        }
        if ("net_listening" == method) {
            return true
        }
        if ("web3_clientVersion" == method) {
            return "EmeraldDshackle/v0.1"
        }
        if ("eth_protocolVersion" == method) {
            return "0x3f"
        }
        if ("eth_syncing" == method) {
            return false
        }
        if ("eth_coinbase" == method) {
            return "0x0000000000000000000000000000000000000000"
        }
        if ("eth_mining" == method) {
            return "false"
        }
        if ("eth_hashrate" == method) {
            return "0x0"
        }
        if ("eth_accounts" == method) {
            return Collections.emptyList<String>()
        }
        throw RpcException(-32601, "Method not found")
    }
}