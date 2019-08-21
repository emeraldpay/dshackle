package io.emeraldpay.dshackle.upstream

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.reader.EmptyReader
import io.emeraldpay.dshackle.reader.Reader
import io.infinitape.etherjar.domain.BlockHash
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.hex.HexQuantity
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.json.ResponseJson
import reactor.core.publisher.Mono
import java.util.function.Function

open class CachingEthereumApi(
        private val objectMapper: ObjectMapper,
        private val cache: Reader<BlockHash, BlockJson<TransactionId>>,
        private val head: EthereumHead
): EthereumApi(objectMapper) {

    companion object {
        @JvmStatic
        fun empty(): CachingEthereumApi {
            return CachingEthereumApi(ObjectMapper(), EmptyReader(), EmptyEthereumHead())
        }
    }

    override fun execute(id: Int, method: String, params: List<Any>): Mono<ByteArray> {
        return when (method) {
            "eth_blockNumber" -> head.getFlux().next()
                    .map { HexQuantity.from(it.number).toHex() }
                    .map(toJson(id))
            "eth_getBlockByHash" -> Mono.just(params[0])
                    .map { BlockHash.from(it as String) }
                    .flatMap(cache::read)
                    .map(toJson(id))
            else -> Mono.empty()
        }
    }

    fun toJson(id: Int): Function<Any, ByteArray> {
        return Function { data ->
            val resp = ResponseJson<Any, Int>()
            resp.id = id
            resp.result = data
            objectMapper.writer().writeValueAsBytes(resp)
        }
    }
}