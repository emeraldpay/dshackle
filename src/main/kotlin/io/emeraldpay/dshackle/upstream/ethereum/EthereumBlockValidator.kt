package io.emeraldpay.dshackle.upstream.ethereum

import com.fasterxml.jackson.databind.JsonNode
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.upstream.BlockValidator
import io.emeraldpay.dshackle.upstream.ethereum.RLP.Companion.encode
import io.emeraldpay.dshackle.upstream.ethereum.RLP.Companion.encodeBigInt
import io.emeraldpay.dshackle.upstream.ethereum.RLP.Companion.encodeList
import io.emeraldpay.dshackle.upstream.ethereum.RLP.Companion.fromHexString
import io.emeraldpay.dshackle.upstream.ethereum.RLP.Companion.fromHexStringI
import org.bouncycastle.jcajce.provider.digest.Keccak
import org.slf4j.LoggerFactory
import java.math.BigInteger

class EthereumBlockValidator : BlockValidator {

    companion object {
        val MAX_BIG_INT: BigInteger = BigInteger.valueOf(2).pow(256)
        val NUMBER_ELEMENTS = setOf("difficulty", "number", "gasLimit", "gasUsed", "timestamp", "baseFeePerGas")
        val ELEMENTS = setOf(
            "parentHash",
            "sha3Uncles",
            "miner",
            "stateRoot",
            "transactionsRoot",
            "receiptsRoot",
            "logsBloom",
            "difficulty",
            "number",
            "gasLimit",
            "gasUsed",
            "timestamp",
            "extraData",
            "mixHash",
            "nonce",
            "baseFeePerGas"
        )

        private val log = LoggerFactory.getLogger(EthereumBlockValidator::class.java)
    }

    override fun isValid(block: BlockContainer): Boolean =
        block.json?.let {
            println(String(it))
            val node = Global.objectMapper.readTree(it)
            val rlpEncoded = rlp(node)
            val hashValid = validateHash(block.hash, rlpEncoded)

            if (!hashValid) {
                log.warn("Hash not valid for block ${block.hash}")
            }
            val difficultyValid = validateDifficulty(node, rlpEncoded)
            if (!difficultyValid) {
                log.warn("PoW not valid for block ${block.hash}")
            }

            hashValid && difficultyValid
        } ?: false

    private fun rlp(node: JsonNode): Map<String, ByteArray> =
        ELEMENTS.mapNotNull {
            node.get(it)?.asText()?.let { text ->
                it to text
            }
        }.associate {
            it.first to if (NUMBER_ELEMENTS.contains(it.first)) encodeBigInt(fromHexStringI(it.second)) else encode(
                fromHexString(it.second)
            )
        }

    private fun validateHash(blockHash: BlockId, rlpEncoded: Map<String, ByteArray>): Boolean {
        val elements = ELEMENTS.mapNotNull { rlpEncoded[it] }.toList()
        val encoded = encodeList(elements)
        return blockHash.value.contentEquals(sha3(encoded))
    }

    private fun validateDifficulty(node: JsonNode, rlpEncoded: Map<String, ByteArray>): Boolean {
        val nonce = fromHexStringI(node["nonce"].asText())
        val difficulty = fromHexStringI(node["difficulty"].asText())
        val mix = fromHexString(node["mixHash"].asText())

        val target = MAX_BIG_INT.divide(difficulty)
        val encoded = encodeList(
            ELEMENTS.filterNot { it in listOf("nonce", "mixHash") }
                .mapNotNull { rlpEncoded[it] }
                .toList()
        )
        val sha3 = sha3(encoded)
        val seed = Keccak.Digest512().digest(sha3.plus(nonce.asUint64()))
        val result = sha3(seed.plus(mix))
        return target >= BigInteger(1, result)
    }

    private fun BigInteger.asUint64() = this.toByteArray().let { bytes ->
        ByteArray(8) { bytes[bytes.size - it - 1] }
    }

    private fun sha3(byteArray: ByteArray): ByteArray =
        Keccak.Digest256().digest(byteArray)
}
