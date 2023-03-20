package io.emeraldpay.dshackle.upstream.bitcoin

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.data.TxContainer
import io.emeraldpay.dshackle.data.TxId
import org.slf4j.LoggerFactory
import java.time.Instant

class ExtractTx {

    companion object {
        private val log = LoggerFactory.getLogger(ExtractTx::class.java)

        @JvmStatic
        fun getTime(data: Map<String, Any>): Instant? {
            val time = data["time"] as Number? ?: return null
            return Instant.ofEpochMilli(time.toLong() * 1000)
        }
    }

    private val objectMapper: ObjectMapper = Global.objectMapper

    @Suppress("UNCHECKED_CAST")
    fun extract(json: ByteArray): TxContainer {
        val data = objectMapper.readValue(json, Map::class.java) as Map<String, Any>

        val txid = data["hash"] as String? ?: throw IllegalArgumentException("Block JSON has no hash")
        val blockhash = data["blockhash"] as String?

        return TxContainer(
            height = null,
            hash = TxId.from(txid),
            blockId = blockhash?.let { BlockId.from(it) },
            json = json,
            parsed = data,
        )
    }
}
