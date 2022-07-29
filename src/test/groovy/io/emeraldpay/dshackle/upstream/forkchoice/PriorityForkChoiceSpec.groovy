package io.emeraldpay.dshackle.upstream.forkchoice

import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import spock.lang.Specification

import java.time.Instant

class PriorityForkChoiceSpec extends Specification {
    def blocks = [1L, 2, 3, 4].collect { i ->
        byte[] hash = new byte[32]
        hash[0] = i as byte
        new BlockContainer(i, BlockId.from(hash), BigInteger.valueOf(i), Instant.now(), false, null, null, [], i.toInteger())
    }
    def "filters blocks"() {
        def choice = new PriorityForkChoice()
        choice.choose(blocks[1])
        expect:
        !choice.filter(blocks[0])
        choice.filter(blocks[2])
        !choice.filter(blocks[1])
    }

    def "chooses correct block according to node rating"() {
        def choice = new PriorityForkChoice()
        choice.choose(blocks[1])
        when:
        choice.choose(blocks[0])
        then:
        choice.getHead() == blocks[1]
        when:
        choice.choose(blocks[2])
        then:
        choice.getHead() == blocks[2]
        when:
        def seenblock = blocks[1].copyWithRating(20)
        choice.choose(seenblock)
        then:
        choice.getHead() == blocks[2]
    }
}
