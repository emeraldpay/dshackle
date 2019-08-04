package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.test.TestingCommons
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.json.BlockJson
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import spock.lang.Specification

class BroadcastQuorumSpec extends Specification {

    def rpcConverted = TestingCommons.rpcConverter()
    def objectMapper = TestingCommons.objectMapper()

    def "Resolved with first after 3 tries"() {
        setup:
        def q = Spy(new BroadcastQuorum(rpcConverted, 3))
        def upstream1 = Stub(Upstream)
        def upstream2 = Stub(Upstream)
        def upstream3 = Stub(Upstream)

        when:
        q.init(Stub(Head))
        then:
        !q.isResolved()

        when:
        q.record(objectMapper.writeValueAsBytes([result: "0xeaa972c0d8d1ecd3e34fbbef6d34e06670e745c788bdba31c4234a1762f0378c"]), upstream1)
        then:
        !q.isResolved()
        1 * q.recordValue(_, "0xeaa972c0d8d1ecd3e34fbbef6d34e06670e745c788bdba31c4234a1762f0378c", _)

        when:
        q.record(objectMapper.writeValueAsBytes([result: "0xeaa972c0d8d1ecd3e34fbbef6d34e06670e745c788bdba31c4234a1762f0378c"]), upstream2)
        then:
        !q.isResolved()
        1 * q.recordValue(_, "0xeaa972c0d8d1ecd3e34fbbef6d34e06670e745c788bdba31c4234a1762f0378c", _)

        when:
        q.record(objectMapper.writeValueAsBytes([error: [message: "Nonce too low"]]), upstream3)
        then:
        1 * q.recordError(_, _, _)
        q.isResolved()
        objectMapper.readValue(q.result, Map) == [result: "0xeaa972c0d8d1ecd3e34fbbef6d34e06670e745c788bdba31c4234a1762f0378c"]
    }

    def "Remembers first response"() {
        setup:
        def q = Spy(new BroadcastQuorum(rpcConverted, 3))
        def upstream1 = Stub(Upstream)
        def upstream2 = Stub(Upstream)
        def upstream3 = Stub(Upstream)

        when:
        q.init(Stub(Head))
        then:
        !q.isResolved()

        when:
        q.record(objectMapper.writeValueAsBytes([error: [message: "Internal error"]]), upstream1)
        then:
        !q.isResolved()
        1 * q.recordError(_, _, _)

        when:
        q.record(objectMapper.writeValueAsBytes([result: "0xeaa972c0d8d1ecd3e34fbbef6d34e06670e745c788bdba31c4234a1762f0378c"]), upstream2)
        then:
        !q.isResolved()
        1 * q.recordValue(_, "0xeaa972c0d8d1ecd3e34fbbef6d34e06670e745c788bdba31c4234a1762f0378c", _)

        when:
        q.record(objectMapper.writeValueAsBytes([error: [message: "Nonce too low"]]), upstream3)
        then:
        1 * q.recordError(_, _, _)
        q.isResolved()
        objectMapper.readValue(q.result, Map) == [result: "0xeaa972c0d8d1ecd3e34fbbef6d34e06670e745c788bdba31c4234a1762f0378c"]
    }
}
