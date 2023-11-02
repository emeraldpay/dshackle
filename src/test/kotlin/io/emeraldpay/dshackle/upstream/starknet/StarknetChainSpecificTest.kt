package io.emeraldpay.dshackle.upstream.starknet

import io.emeraldpay.dshackle.data.BlockId
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test

val example = """
    {
        "status": "ACCEPTED_ON_L2",
        "block_hash": "0x46fa6638dc7fae06cece980ce4195436a79ef314ca49d99e0cef552d6f13c4e",
        "parent_hash": "0x7cc1e178c848b0bdfa047a414d9a8bee4f6cb76f25f312f2d42e058ea91d78b",
        "block_number": 304789,
        "new_root": "0x71ba085a6fb172ccb206347eec04c9c171e770c947968fc9df31297e4cee145",
        "timestamp": 1696802363,
        "sequencer_address": "0x1176a1bd84444c89232ec27754698e5d2e7e1a7f1539f12027f28b23ec9f3d8",
        "transactions": [
          "0x3303c7acaa3e6efc0cd30f0e4be41a4df1117958f0bdc32cd0759b3664922ed"
        ]
    }
""".trimIndent()

class StarknetChainSpecificTest {
    @Test
    fun parseResponse() {
        val result = StarknetChainSpecific.parseBlock(example.toByteArray(), "1")

        Assertions.assertThat(result.height).isEqualTo(304789)
        Assertions.assertThat(result.hash).isEqualTo(BlockId.from("046fa6638dc7fae06cece980ce4195436a79ef314ca49d99e0cef552d6f13c4e"))
        Assertions.assertThat(result.upstreamId).isEqualTo("1")
        Assertions.assertThat(result.parentHash).isEqualTo(BlockId.from("07cc1e178c848b0bdfa047a414d9a8bee4f6cb76f25f312f2d42e058ea91d78b"))
    }
}
