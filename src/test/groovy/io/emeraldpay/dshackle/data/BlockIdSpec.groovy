package io.emeraldpay.dshackle.data

import io.infinitape.etherjar.domain.BlockHash
import spock.lang.Specification

class BlockIdSpec extends Specification {

    def "Create from string"() {
        expect:
        BlockId.from(source).toHex() == exp
        where:
        exp                                                                | source
        "a0e65cbc1b52a8ca60562112c6060552d882f16f34a9dba2ccdc05c0a6a27100" | "a0e65cbc1b52a8ca60562112c6060552d882f16f34a9dba2ccdc05c0a6a27100"
        "a0e65cbc1b52a8ca60562112c6060552d882f16f34a9dba2ccdc05c0a6a27100" | "A0E65CBC1B52A8CA60562112C6060552D882F16F34A9DBA2CCDC05C0A6A27100"
        "a0e65cbc1b52a8ca60562112c6060552d882f16f34a9dba2ccdc05c0a6a27100" | "0xa0e65cbc1b52a8ca60562112c6060552d882f16f34a9dba2ccdc05c0a6a27100"
        "a0e65cbc1b52a8ca60562112c6060552d882f16f34a9dba2ccdc05c0a6a27100" | "0xA0E65CBC1B52A8CA60562112C6060552D882F16F34A9DBA2CCDC05C0A6A27100"
        "00e65cbc1b52a8ca60562112c6060552d882f16f34a9dba2ccdc05c0a6a27100" | "00e65cbc1b52a8ca60562112c6060552d882f16f34a9dba2ccdc05c0a6a27100"
        "00e65cbc1b52a8ca60562112c6060552d882f16f34a9dba2ccdc05c0a6a27100" | "0x00e65cbc1b52a8ca60562112c6060552d882f16f34a9dba2ccdc05c0a6a27100"
    }

    def "Create from block hash"() {
        expect:
        BlockId.from(BlockHash.from(source)).toHex() == exp
        where:
        exp                                                                | source
        "a0e65cbc1b52a8ca60562112c6060552d882f16f34a9dba2ccdc05c0a6a27100" | "0xa0e65cbc1b52a8ca60562112c6060552d882f16f34a9dba2ccdc05c0a6a27100"
        "00e65cbc1b52a8ca60562112c6060552d882f16f34a9dba2ccdc05c0a6a27100" | "0x00e65cbc1b52a8ca60562112c6060552d882f16f34a9dba2ccdc05c0a6a27100"
    }

}
