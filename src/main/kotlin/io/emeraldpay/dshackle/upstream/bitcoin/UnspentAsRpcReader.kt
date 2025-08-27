package io.emeraldpay.dshackle.upstream.bitcoin

import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.SilentException
import io.emeraldpay.dshackle.reader.DshackleRpcReader
import io.emeraldpay.dshackle.upstream.bitcoin.data.RpcUnspent
import io.emeraldpay.dshackle.upstream.bitcoin.data.SimpleUnspent
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleRequest
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleResponse
import org.bitcoinj.core.Address
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono

class UnspentAsRpcReader(
    private val unspentReader: UnspentReader,
) : DshackleRpcReader {
    companion object {
        private val log = LoggerFactory.getLogger(UnspentAsRpcReader::class.java)
    }

    override fun read(key: DshackleRequest): Mono<DshackleResponse> {
        if (key.method == "listunspent") {
            return processUnspentRequest(key)
                .map { DshackleResponse(id = key.id, result = it, error = null) }
        }
        log.warn("Unspent Reader called with ${key.method} request")
        return Mono.empty()
    }

    /**
     *
     */
    fun processUnspentRequest(key: DshackleRequest): Mono<ByteArray> {
        if (key.params.size < 3) {
            return Mono.error(SilentException("Invalid call to unspent. Address is missing"))
        }
        val addresses = key.params[2]
        if (addresses is List<*> && addresses.size > 0) {
            val address = addresses[0].toString().let { Address.fromString(null, it) }
            return unspentReader.read(address).map {
                val rpc = it.map(convertUnspent(address))
                Global.objectMapper.writeValueAsBytes(rpc)
            }
        }
        return Mono.error(SilentException("Invalid call to unspent"))
    }

    fun convertUnspent(address: Address): (SimpleUnspent) -> RpcUnspent =
        { base ->
            RpcUnspent(
                base.txid,
                base.vout,
                address.toString(),
                base.value,
            )
        }
}
