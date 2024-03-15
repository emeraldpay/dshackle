package io.emeraldpay.dshackle.upstream.beaconchain

import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.math.BigInteger
import java.time.Instant

class BeaconChainSpecificTest {

    @Test
    fun `test getting latest block`() {
        val header = """
            {
                "execution_optimistic": false,
                "finalized": false,
                "data": {
                    "header": {
                        "message": {
                            "slot": "8639623",
                            "proposer_index": "830020",
                            "parent_root": "0x3306b3f2d5f4de709a9b13c51ca7bbeab41ee93f44c6873baf6a59495620dbbf",
                            "state_root": "0x1445392736bd2485e5afaaea62a642bea69f75379653e7a1f0846806af8d6d75",
                            "body_root": "0x0a02f2980fda874a050fc2f6fde63ebbeb749318f23325673f1fc4af2b252e84"
                        },
                        "signature": "0x961b9c71530f2a55630e3c943aa054f6a863c86fd53b6644c1680bdb2d70a65a0082e5282fb3bf54a0167ba706fe78961289a652da13594a00d63c4ef8d60f53f515ce5ca1ced41b25a7689fd6e6773ec6b771e4e1f67c37eea2b8ef26c5eb4f"
                    },
                    "root": "0x3ae4855df015bf9906f59aa0c612396105fa7c11495b5596fa946672c3ce5674",
                    "canonical": true
                }
            }
        """.trimIndent().toByteArray()
        val expected = BlockContainer(
            height = 8639623,
            hash = BlockId.from("0x3ae4855df015bf9906f59aa0c612396105fa7c11495b5596fa946672c3ce5674"),
            difficulty = BigInteger.ZERO,
            timestamp = Instant.EPOCH,
            full = false,
            json = header,
            transactions = emptyList(),
            upstreamId = "upId",
            parentHash = BlockId.from("0x3306b3f2d5f4de709a9b13c51ca7bbeab41ee93f44c6873baf6a59495620dbbf"),
            parsed = null,
        )

        val block = BeaconChainSpecific.parseBlock(header, "upId")
        assertEquals(expected, block)
    }
}
