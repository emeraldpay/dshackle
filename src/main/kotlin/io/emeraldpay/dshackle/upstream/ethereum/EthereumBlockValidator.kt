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
import io.emeraldpay.etherjar.rpc.json.BlockJson
import org.bouncycastle.jcajce.provider.digest.Keccak
import org.slf4j.LoggerFactory
import java.math.BigDecimal
import java.math.BigInteger

class EthereumBlockValidator : BlockValidator {

    companion object {
        val ACCURACY = 0.02.toBigDecimal()
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

    override fun isValid(currentHead: BlockContainer?, newHead: BlockContainer): Boolean =
        newHead.json?.let {
            val validBlocksSequence = currentHead?.let { cur -> validateTotalDifficulty(cur, newHead) } ?: true
            val node = Global.objectMapper.readTree(it)
            val rlpEncoded = rlp(node)
            val hashValid = validateHash(newHead.hash, rlpEncoded)
            if (!hashValid) {
                log.warn("Hash '${newHead.hash}' not valid for block ${newHead.hash}")
            }
            val difficultyValid = validateDifficulty(node, rlpEncoded)
            if (!difficultyValid) {
                log.warn("PoW not valid for block ${newHead.hash}")
            }

            hashValid && difficultyValid && validBlocksSequence
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
        val difficulty = fromHexStringI(node["difficulty"].asText())
        if (difficulty.compareTo(BigInteger.ZERO) == 0 || difficulty.signum() < 0) {
            return false
        }
        val nonce = fromHexStringI(node["nonce"].asText())
        val mix = fromHexString(node["mixHash"].asText())

        val target = MAX_BIG_INT.divide(difficulty)
        val encoded = encodeList(
            ELEMENTS.filterNot { it in listOf("nonce", "mixHash") }
                .mapNotNull { rlpEncoded[it] }
                .toList()
        )
        val headerHash = sha3(encoded)
        val seed = Keccak.Digest512().digest(headerHash.plus(nonce.asUint64()))
        val result = sha3(seed.plus(mix))
        return target >= BigInteger(1, result)
    }

    private fun validateTotalDifficulty(currentHead: BlockContainer, newHead: BlockContainer): Boolean =
        currentHead.getParsed(BlockJson::class.java)?.let { cur ->
            newHead.getParsed(BlockJson::class.java)?.let {
                val numberValid = it.number >= cur.number
                if (!numberValid) {
                    log.warn(
                        "Block number {} not valid for {}. Must be greater than {}",
                        it.number,
                        it.hash,
                        cur.number
                    )
                }

                val timestampValid = it.timestamp > cur.timestamp
                if (!timestampValid) {
                    log.warn(
                        "Block timestamp {} not valid for {}. Must be greater than {}",
                        it.timestamp,
                        it.hash,
                        cur.timestamp
                    )
                }

                numberValid && timestampValid && approxTotalDifficultyValid(
                    curTotalDifficulty = cur.totalDifficulty,
                    curDifficulty = cur.difficulty,
                    curNumber = cur.number,
                    blockTotalDifficulty = it.totalDifficulty,
                    blockNumber = it.number,
                    blockHash = it.hash.toHex()
                )
            } ?: false
        } ?: true

    private fun approxTotalDifficultyValid(
        curTotalDifficulty: BigInteger,
        curDifficulty: BigInteger,
        curNumber: Long,
        blockTotalDifficulty: BigInteger,
        blockNumber: Long,
        blockHash: String
    ): Boolean {
        if (blockTotalDifficulty < curTotalDifficulty) {
            log.warn(
                "Block totalDifficulty {} not valid for {}. Must be greater than {}",
                blockTotalDifficulty,
                blockHash,
                curTotalDifficulty
            )
            return false
        }

        val decimalRef = curDifficulty.toBigDecimal()
        val shift = BigDecimal.valueOf(blockNumber - curNumber)
        val totalAccuracy = shift * ACCURACY

        val blockApprox = (blockTotalDifficulty - curTotalDifficulty).toBigDecimal() / shift
        val min = decimalRef * (BigDecimal.ONE - totalAccuracy)
        val max = decimalRef * (BigDecimal.ONE + totalAccuracy)

        val valid = blockApprox in min..max
        if (!valid) {
            log.warn(
                "Block totalDifficulty {} not valid for {}. Expected to be int [{}, {}]",
                blockTotalDifficulty,
                blockHash,
                min.toBigInteger(),
                max.toBigInteger()
            )
        }
        return valid
    }

    private fun BigInteger.asUint64() = this.toByteArray().let { bytes ->
        ByteArray(8) {
            val index = bytes.size - it - 1
            if (index < bytes.size && index >= 0) bytes[index] else 0
        }
    }

    private fun sha3(byteArray: ByteArray): ByteArray =
        Keccak.Digest256().digest(byteArray)
}
