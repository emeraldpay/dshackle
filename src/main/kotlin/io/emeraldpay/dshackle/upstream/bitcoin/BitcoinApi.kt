package io.emeraldpay.dshackle.upstream.bitcoin

import com.fasterxml.jackson.databind.JavaType
import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.upstream.UpstreamApi
import io.infinitape.etherjar.rpc.RpcException
import io.infinitape.etherjar.rpc.json.FullResponseJson
import io.infinitape.etherjar.rpc.json.RequestJson
import io.infinitape.etherjar.rpc.json.ResponseJson
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono

open class BitcoinApi(
        val bitcoinRpcClient: BitcoinRpcClient,
        val objectMapper: ObjectMapper
) : UpstreamApi {

    companion object {
        private val log = LoggerFactory.getLogger(BitcoinApi::class.java)
    }

    open override fun execute(id: Int, method: String, params: List<Any>): Mono<ByteArray> {
        //TODO optimize extraction
        return executeAndResult(id, method, params, Object::class.java).map {
            objectMapper.writeValueAsBytes(it)
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
}