package io.emeraldpay.dshackle.upstream.ethereum


import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.etherjar.rpc.json.BlockJson
import org.apache.commons.io.FileUtils
import spock.lang.Specification

class EthereumBlockValidatorSpec extends Specification {

    def mapper = Global.getObjectMapper()
    def validator = new EthereumBlockValidator()

    def "test validate block"() {
        expect:
        def path = "src/test/resources/blocks/${file}.json"
        validator.isValid(null, fromFile(path)) == expect

        where:
        file                      || expect
        "eth_valid_block_1"       || true
        "eth_block_sec_1"         || true
        "eth_block_sec_2"         || true
        "eth_block_sec_2_invalid" || true
    }

    def "test validate blocks sequence"() {
        expect:
        def curPath = "src/test/resources/blocks/${cur}.json"
        def nextPath = "src/test/resources/blocks/${next}.json"

        validator.isValid(fromFile(curPath), fromFile(nextPath)) == expect

        where:
        cur               || next                      || expect
        "eth_block_sec_1" || "eth_block_sec_2"         || true
        "eth_block_sec_2" || "eth_block_sec_1"         || false
        "eth_block_sec_1" || "eth_block_sec_2_invalid" || false
    }

    def fromFile(String path) {
        def bytes = FileUtils.readFileToByteArray(new File(path))
        def block = mapper.readValue(bytes, BlockJson.class)

        return new BlockContainer(
                block.number,
                BlockId.from(block.hash.toHex()),
                block.difficulty,
                block.timestamp,
                true, bytes,
                block,
                Collections.emptyList(),
                1,
                "upstream"
        )
    }
}
