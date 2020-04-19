package io.emeraldpay.dshackle.upstream.bitcoin

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.data.TxId
import org.apache.commons.codec.binary.Hex
import org.slf4j.LoggerFactory
import java.math.BigInteger
import java.time.Instant

class ExtractBlock(
        private val objectMapper: ObjectMapper
) {

    companion object {
        private val log = LoggerFactory.getLogger(ExtractBlock::class.java)
    }

    fun extract(json: ByteArray): BlockContainer {
        val data = objectMapper.readValue(json, Map::class.java) as Map<String, Any>

        val height = data["height"] as Number? ?: throw IllegalArgumentException("Block JSON has no height")
        val time = data["time"] as Number? ?: throw IllegalArgumentException("Block JSON has no time")
        val hash = data["hash"] as String? ?: throw IllegalArgumentException("Block JSON has no hash")
        val chainwork = data["chainwork"] as String? ?: throw IllegalArgumentException("Block JSON has no chainwork")
        val transactions = (data["tx"] as List<String>?)?.map(TxId.Companion::from) ?: emptyList()

        return BlockContainer(
                height.toLong(),
                BlockId.from(hash),
                BigInteger(1, Hex.decodeHex(chainwork)),
                Instant.ofEpochMilli(time.toLong() * 1000),
                false,
                json,
                transactions
        )
    }

}