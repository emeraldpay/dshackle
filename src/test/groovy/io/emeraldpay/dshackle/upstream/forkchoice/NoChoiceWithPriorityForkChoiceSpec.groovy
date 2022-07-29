package io.emeraldpay.dshackle.upstream.forkchoice

import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import spock.lang.Specification

import java.time.Instant

class NoChoiceWithPriorityForkChoiceSpec extends Specification {
    def blocks = [1L, 2, 3, 4].collect { i ->
        byte[] hash = new byte[32]
        hash[0] = i as byte
        new BlockContainer(i, BlockId.from(hash), BigInteger.valueOf(i), Instant.now(), false, null, null, [], 0)
    }

    def "filters blocks"() {
        def blockR0 = blocks[0].copyWithRating(10)
        def blockR1 = blocks[1].copyWithRating(10)
        def choice = new NoChoiceWithPriorityForkChoice(10)
        when:
        choice.choose(blocks[0])
        then:
        choice.getHead() == blockR0
        when:
        choice.choose(blocks[1])
        then:
        choice.getHead() == blockR1
        when:
        choice.choose(blocks[0])
        then:
        choice.getHead() == blocks[1]
    }

    def "chooses blocks and adds rating"() {
        def blockR0 = blocks[0].copyWithRating(10)
        def blockR1 = blocks[1].copyWithRating(10)
        def choice = new NoChoiceWithPriorityForkChoice(10)
        when:
        choice.choose(blocks[0])
        then:
        choice.getHead() == blockR0
        when:
        choice.choose(blocks[1])
        then:
        choice.getHead() == blockR1
        when:
        choice.choose(blocks[0])
        then:
        choice.getHead() == blockR1
    }
}
