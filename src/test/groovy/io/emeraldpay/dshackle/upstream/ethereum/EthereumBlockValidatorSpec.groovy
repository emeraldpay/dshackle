package io.emeraldpay.dshackle.upstream.ethereum

import com.fasterxml.jackson.databind.JsonNode
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import org.apache.commons.io.FileUtils
import spock.lang.Specification

import java.time.Instant

class EthereumBlockValidatorSpec extends Specification {

    def validator = new EthereumBlockValidator()

    def "test validate block"() {
        expect:
        def path = "src/test/resources/blocks/${file}.json"
        def bytes = FileUtils.readFileToByteArray(new File(path))

        def container = new BlockContainer(1L, BlockId.from(hash), BigInteger.ONE, Instant.now(), false, bytes, null, Collections.emptyList(), 1)
        validator.isValid(container) == expect

        where:
        file                || expect || hash
        "eth_valid_block_1" || true   || "0xae174c5cf816f820b17ae13d120fff057406ce12f448422f0d522459d4ae646b"
        "eth_valid_block_2" || true   || "0x95fd7b85ed04cc67884d7c33d13df3ebb568a2c1532021316c296038bda71776"
    }
}
