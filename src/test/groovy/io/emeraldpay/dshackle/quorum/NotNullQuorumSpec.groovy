package io.emeraldpay.dshackle.quorum

import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcException
import io.emeraldpay.dshackle.upstream.signature.ResponseSigner
import spock.lang.Specification

class NotNullQuorumSpec extends Specification {

    def "Resolves if attempts are exhausted and response is null"() {
        setup:
        def up = Mock(Upstream) {
            2 * getId() >> "id"
        }
        def up1 = Mock(Upstream) {
            1 * getId() >> "id1"
        }
        def up2 = Mock(Upstream) {
            1 * getId() >> "id2"
        }
        def value = "null".getBytes()
        def quorum = new NotNullQuorum()

        when:
        def res = quorum.record(value, new ResponseSigner.Signature("sig1".bytes, "test", 100), up, "id")
        def res1 = quorum.record(value, new ResponseSigner.Signature("sig1".bytes, "test", 100), up1, "id1")
        def res2 = quorum.record(value, new ResponseSigner.Signature("sig1".bytes, "test", 100), up2, "id2")
        def res3 = quorum.record(value, new ResponseSigner.Signature("sig1".bytes, "test", 100), up, "id")
        then:
        !res
        !res1
        !res2
        res3
        quorum.result == value
        !quorum.isFailed()
        quorum.isResolved()
        quorum.signature == new ResponseSigner.Signature("sig1".bytes, "test", 100)
        quorum.providedUpstreamId == "id"
    }

    def "Failed if all upstreams respond with error"() {
        setup:
        def up = Mock(Upstream) {
            2 * getId() >> "id"
        }
        def up1 = Mock(Upstream) {
            1 * getId() >> "id1"
        }
        def up2 = Mock(Upstream) {
            1 * getId() >> "id2"
        }
        def quorum = new NotNullQuorum()

        when:
        quorum.record(new JsonRpcException(10, "error"), null, up)
        quorum.record(new JsonRpcException(10, "error"), null, up1)
        quorum.record(new JsonRpcException(10, "error"), null, up2)
        quorum.record(new JsonRpcException(10, "error"), null, up)

        then:
        quorum.isFailed()
        !quorum.isResolved()
        quorum.error == new JsonRpcException(10, "error").error
    }

    def "Resolve if one of upstream responds with value"() {
        setup:
        def up = Mock(Upstream) {
            2 * getId() >> "id"
        }
        def up1 = Mock(Upstream) {
            1 * getId() >> "id1"
        }
        def up2 = Mock(Upstream) {
            1 * getId() >> "id2"
        }
        def value = "null".getBytes()
        def quorum = new NotNullQuorum()

        when:
        def res = quorum.record(value, new ResponseSigner.Signature("sig1".bytes, "test", 100), up, "id")
        quorum.record(new JsonRpcException(10, "error"), new ResponseSigner.Signature("sig1".bytes, "test", 100), up1)
        quorum.record(new JsonRpcException(10, "error"), new ResponseSigner.Signature("sig1".bytes, "test", 100), up2)
        quorum.record(new JsonRpcException(10, "error"), new ResponseSigner.Signature("sig1".bytes, "test", 100), up)

        then:
        !res
        quorum.isResolved()
        !quorum.isFailed()
        quorum.result == value
        quorum.signature == new ResponseSigner.Signature("sig1".bytes, "test", 100)
    }
}
