package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.test.TestingCommons
import spock.lang.Specification

class NonceQuorumSpec extends Specification {

    def rpcConverted = TestingCommons.rpcConverter()
    def objectMapper = TestingCommons.objectMapper()

    def "Gets max value"() {
        setup:
        def q = Spy(new NonceQuorum(rpcConverted, 3))
        def upstream1 = Stub(Upstream)
        def upstream2 = Stub(Upstream)
        def upstream3 = Stub(Upstream)

        when:
        q.init(Stub(Head))
        then:
        !q.isResolved()

        when:
        q.record(objectMapper.writeValueAsBytes([result: "0x10"]), upstream1)
        then:
        !q.isResolved()
        1 * q.recordValue(_, "0x10", _)

        when:
        q.record(objectMapper.writeValueAsBytes([result: "0x11"]), upstream2)
        then:
        !q.isResolved()
        1 * q.recordValue(_, "0x11", _)

        when:
        q.record(objectMapper.writeValueAsBytes([result: "0x10"]), upstream3)
        then:
        1 * q.recordValue(_, "0x10", _)
        q.isResolved()
        objectMapper.readValue(q.result, Map) == [result: "0x11"]
    }

    def "Ignores errors"() {
        setup:
        def q = Spy(new NonceQuorum(rpcConverted, 3))
        def upstream1 = Stub(Upstream)
        def upstream2 = Stub(Upstream)
        def upstream3 = Stub(Upstream)

        when:
        q.init(Stub(Head))
        then:
        !q.isResolved()

        when:
        q.record(objectMapper.writeValueAsBytes([error: [error: "Internal"]]), upstream1)
        then:
        !q.isResolved()
        1 * q.recordError(_, _, _)

        when:
        q.record(objectMapper.writeValueAsBytes([result: "0x11"]), upstream2)
        then:
        !q.isResolved()
        1 * q.recordValue(_, "0x11", _)

        when:
        q.record(objectMapper.writeValueAsBytes([result: "0x10"]), upstream3)
        then:
        1 * q.recordValue(_, "0x10", _)
        !q.isResolved()

        when:
        q.record(objectMapper.writeValueAsBytes([result: "0x11"]), upstream1)
        then:
        1 * q.recordValue(_, "0x11", _)
        q.isResolved()
        objectMapper.readValue(q.result, Map) == [result: "0x11"]
    }
}
