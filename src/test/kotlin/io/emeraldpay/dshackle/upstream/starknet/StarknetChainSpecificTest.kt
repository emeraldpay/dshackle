package io.emeraldpay.dshackle.upstream.starknet

import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.reader.ChainReader
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.ChainResponse
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test
import reactor.core.publisher.Mono

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

val syncingGood = """
    {
    "current_block_hash": "0x2672c3bd6afd698cf979ead19e486cad0c113e1ea7c55467a4c514128b3fe2c",
    "current_block_num": 426006,
    "highest_block_hash": "0x2672c3bd6afd698cf979ead19e486cad0c113e1ea7c55467a4c514128b3fe2c",
    "highest_block_num": 426006,
    "starting_block_hash": "0x43734ae66b4f11afa254d0ac5e8f3bc599638c8c92ca97f82e680c4a012005a",
    "starting_block_num": 397751
  }
""".trimIndent()

val syncingBad = """
    {
    "current_block_hash": "0x2672c3bd6afd698cf979ead19e486cad0c113e1ea7c55467a4c514128b3fe2c",
    "current_block_num": 426006,
    "highest_block_hash": "0x2672c3bd6afd698cf979ead19e486cad0c113e1ea7c55467a4c514128b3fe2c",
    "highest_block_num": 526006,
    "starting_block_hash": "0x43734ae66b4f11afa254d0ac5e8f3bc599638c8c92ca97f82e680c4a012005a",
    "starting_block_num": 397751
  }
""".trimIndent()

class StarknetChainSpecificTest {
    @Test
    fun parseResponse() {
        val result = StarknetChainSpecific.parseBlock(
            example.toByteArray(),
            "1",
            object : ChainReader {
                override fun read(key: ChainRequest): Mono<ChainResponse> = Mono.empty()
            },
        ).block()!!

        Assertions.assertThat(result.height).isEqualTo(304789)
        Assertions.assertThat(result.hash)
            .isEqualTo(BlockId.from("046fa6638dc7fae06cece980ce4195436a79ef314ca49d99e0cef552d6f13c4e"))
        Assertions.assertThat(result.upstreamId).isEqualTo("1")
        Assertions.assertThat(result.parentHash)
            .isEqualTo(BlockId.from("07cc1e178c848b0bdfa047a414d9a8bee4f6cb76f25f312f2d42e058ea91d78b"))
    }

    @Test
    fun validateOk() {
        Assertions.assertThat(StarknetChainSpecific.validate(syncingGood.toByteArray(), 10, "test"))
            .isEqualTo(UpstreamAvailability.OK)
    }

    @Test
    fun validateSyncing() {
        Assertions.assertThat(StarknetChainSpecific.validate(syncingBad.toByteArray(), 10, "test"))
            .isEqualTo(UpstreamAvailability.SYNCING)
    }
}
