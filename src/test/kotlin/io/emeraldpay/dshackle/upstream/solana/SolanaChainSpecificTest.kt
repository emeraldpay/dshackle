package io.emeraldpay.dshackle.upstream.solana

import io.emeraldpay.dshackle.data.BlockId
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test

val example = """{
      "context": {
        "slot": 112301554
      },
      "value": {
        "slot": 112301554,
        "block": {
          "previousBlockhash": "GJp125YAN4ufCSUvZJVdCyWQJ7RPWMmwxoyUQySydZA",
          "blockhash": "6ojMHjctdqfB55JDpEpqfHnP96fiaHEcvzEQ2NNcxzHP",
          "parentSlot": 112301553,
          "blockTime": 1639926816,
          "blockHeight": 101210751
        },
        "err": null
      }
    }
""".trimIndent()
class SolanaChainSpecificTest {

    @Test
    fun parseBlock() {
        val result = SolanaChainSpecific.parseHeader(example.toByteArray(), "1")

        Assertions.assertThat(result.height).isEqualTo(101210751)
        Assertions.assertThat(result.hash).isEqualTo(BlockId.fromBase64("6ojMHjctdqfB55JDpEpqfHnP96fiaHEcvzEQ2NNcxzHP"))
        Assertions.assertThat(result.upstreamId).isEqualTo("1")
        Assertions.assertThat(result.parentHash).isEqualTo(BlockId.fromBase64("GJp125YAN4ufCSUvZJVdCyWQJ7RPWMmwxoyUQySydZA"))
    }
}
