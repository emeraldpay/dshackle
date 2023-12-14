package io.emeraldpay.dshackle.quorum

import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcException
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import spock.lang.Specification

class MaximumValueQuorumSpec extends Specification {
    def "selects maximum from 3 values"() {
        setup:
        def up = Mock(Upstream) {
            getId() >> "id"
        }
        def up1 = Mock(Upstream) {
            getId() >> "id1"
        }
        def up2 = Mock(Upstream) {
            getId() >> "id2"
        }
        when:
        def quorum = new MaximumValueQuorum()
        quorum.record(new JsonRpcResponse('"0x137"'.bytes, null), null, up)
        quorum.record(new JsonRpcResponse('"0x138"'.bytes, null), null, up1)
        quorum.record(new JsonRpcResponse('"0x139"'.bytes, null), null, up2)
        then:
        quorum.response.result == '"0x139"'.bytes
        quorum.resolvedBy.size() == 1
        quorum.isResolved()
        quorum.resolvedBy.contains(up2)
    }

    def "selects maximum from 2 values and an error"() {
        setup:
        def up = Mock(Upstream) {
            getId() >> "id"
        }
        def up1 = Mock(Upstream) {
            getId() >> "id1"
        }
        def up2 = Mock(Upstream) {
            getId() >> "id2"
        }
        when:
        def quorum = new MaximumValueQuorum()
        quorum.record(new JsonRpcResponse('"0x137"'.bytes, null), null, up)
        quorum.record(new JsonRpcResponse('"0x138"'.bytes, null), null, up1)
        quorum.record(new JsonRpcException(10, "error"), null, up2)
        then:
        quorum.response.result == '"0x138"'.bytes
        quorum.isResolved()
        quorum.resolvedBy.size() == 1
        quorum.resolvedBy.contains(up1)
    }

    def "returns error is all error"() {
        setup:
        def up = Mock(Upstream) {
            getId() >> "id"
        }
        def up1 = Mock(Upstream) {
            getId() >> "id1"
        }
        def up2 = Mock(Upstream) {
            getId() >> "id2"
        }
        when:
        def quorum = new MaximumValueQuorum()
        quorum.record(new JsonRpcException(10, "error1"), null, up)
        quorum.record(new JsonRpcException(10, "error2"), null, up1)
        quorum.record(new JsonRpcException(10, "error3"), null, up2)
        then:
        !quorum.isResolved()
        quorum.isFailed()
        quorum.error == new JsonRpcException(10, "error3").error
        quorum.resolvedBy.size() == 3
    }

}
