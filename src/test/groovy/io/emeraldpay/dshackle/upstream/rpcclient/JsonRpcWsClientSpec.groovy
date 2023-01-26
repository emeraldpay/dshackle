package io.emeraldpay.dshackle.upstream.rpcclient

import io.emeraldpay.dshackle.upstream.ethereum.WsConnection
import io.emeraldpay.dshackle.upstream.ethereum.WsConnectionPool
import reactor.core.Exceptions
import spock.lang.Specification

import java.time.Duration

class JsonRpcWsClientSpec extends Specification {

    def "Produce error if WS is not connected"() {
        setup:
        def ws = Mock(WsConnection)
        def pool = Mock(WsConnectionPool) {
            getConnection() >> ws
        }
        def client = new JsonRpcWsClient(pool)
        when:
        client.read(new JsonRpcRequest("foo_bar", [], 1))
                .block(Duration.ofSeconds(1))
        then:
        def t = thrown(Exceptions.ReactiveException)
        t.cause instanceof JsonRpcException
        1 * ws.isConnected() >> false
    }
}
