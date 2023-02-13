package io.emeraldpay.dshackle.upstream.rpcclient

import io.emeraldpay.etherjar.rpc.RpcException
import nl.jqno.equalsverifier.EqualsVerifier
import spock.lang.Specification

class JsonRpcErrorSpec extends Specification {

    def "Build from RpcException"() {
        when:
        def act = JsonRpcError.from(new RpcException(-32123, "test test"))
        then:
        act.code == -32123
        act.message == "test test"
        act.details == null
    }

    def "Build from RpcException with details"() {
        when:
        def act = JsonRpcError.from(new RpcException(-32123, "test test", "foo bar"))
        then:
        act.code == -32123
        act.message == "test test"
        act.details == "foo bar"
    }

    def "Equals"() {
        when:
        def v = EqualsVerifier.forClass(JsonRpcError)
        then:
        v.verify()
    }
}
