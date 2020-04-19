package io.emeraldpay.dshackle.upstream.bitcoin

import io.emeraldpay.dshackle.test.TestingCommons
import spock.lang.Specification

class ExtractBlockSpec extends Specification {

    ExtractBlock extractBlock = new ExtractBlock(TestingCommons.objectMapper())

    def "Extract standard block"() {
        setup:
        def json = this.class.getClassLoader().getResourceAsStream("bitcoin/block-626472.json").bytes
        when:
        def act = extractBlock.extract(json)
        then:
        act.hash.toHex() == "0000000000000000000889c2e52ca5e1cecac60bce9a3754201a7a9a67791e90"
        act.height == 626472
        act.timestamp.toString() == "2020-04-18T00:58:46Z"
        act.difficulty.toString(16) == "e9baec5f63190a2b0bbcf6b"
        !act.full
        act.transactions.size() == 1487
        act.json == json
    }
}
