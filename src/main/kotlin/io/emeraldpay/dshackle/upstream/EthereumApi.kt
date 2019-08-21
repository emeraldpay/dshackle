package io.emeraldpay.dshackle.upstream

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.hex.HexQuantity
import io.infinitape.etherjar.rpc.*
import io.infinitape.etherjar.rpc.json.ResponseJson
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import java.io.InputStream
import java.time.Duration

abstract class EthereumApi(
        objectMapper: ObjectMapper
) {

    private val jacksonRpcConverter = JacksonRpcConverter(objectMapper)
    var upstream: Upstream? = null

    abstract fun execute(id: Int, method: String, params: List<Any>): Mono<ByteArray>

    fun <JS, RS> executeAndConvert(rpcCall: RpcCall<JS, RS>): Mono<RS> {
        val convertToJS = java.util.function.Function<ByteArray, Mono<JS>> { resp ->
            val inputStream: InputStream = resp.inputStream()
            val jsonValue: JS? = jacksonRpcConverter.fromJson(inputStream, rpcCall.jsonType, Int::class.java)
            if (jsonValue == null) Mono.empty<JS>()
            else Mono.just(jsonValue)
        }
        return execute(0, rpcCall.method, rpcCall.params as List<Any>)
                .flatMap(convertToJS)
                .map(rpcCall.converter::apply)
    }
}