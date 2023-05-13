package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.test.MockWSServer
import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.grpc.Chain
import reactor.test.StepVerifier
import spock.lang.Shared
import spock.lang.Specification

import java.time.Duration

class WsConnectionImplRealSpec extends Specification {

    static SLEEP = 500

    static int port = 19900 + new Random().nextInt(100)
    @Shared
    MockWSServer server
    @Shared
    WsConnectionImpl conn

    def setup() {
        if (System.getenv("CI") == "true") {
            println("RUN IN CI ENVIRONMENT")
            // needs large timeouts on CI where it's much slower to run
            SLEEP = 1500
        }
        port++
        server = new MockWSServer(port)
        server.start()
        Thread.sleep(SLEEP)
        conn = new EthereumWsFactory("test", Chain.ETHEREUM, "ws://localhost:${port}".toURI(), "http://localhost:${port}".toURI())
                .create(null) as WsConnectionImpl
    }

    def cleanup() {
        conn.close()
        server.stop()
    }

    def "Can make a RPC request"() {
        when:
        conn.connect()
        def resp = conn.callRpc(new JsonRpcRequest("foo_bar", []))
        then:
        StepVerifier.create(resp)
                .then {
                    server.onNextReply('{"jsonrpc":"2.0", "id":100, "result": "baz"}')
                }
                .expectNextMatches {
                    it.hasResult() && it.resultAsProcessedString == "baz"
                }
                .expectComplete()
                .verify(Duration.ofSeconds(5))

        when:
        Thread.sleep(SLEEP)
        def act = server.received
        then:
        act.size() == 1
        act[0].value.contains("\"method\":\"foo_bar\"")
    }

    def "Reconnects after server disconnect"() {
        when:
        conn.connect()
        conn.reconnectIntervalSeconds = 2
        Thread.sleep(SLEEP)
        server.stop()
        Thread.sleep(SLEEP)
        server = new MockWSServer(port)
        server.start()
        server.onNextReply('{"jsonrpc":"2.0","id":100,"result":1}')
        // reconnects in 2 seconds, give 1 extra
        Thread.sleep(3_000)
        def resp = conn.callRpc(new JsonRpcRequest("foo_bar", [])).block(Duration.ofSeconds(1))
        def act = server.received

        then:
        act.size() == 1
        act[0].value.contains("\"method\":\"foo_bar\"")
    }

    def "Error on request when server disconnects"() {
        when:
        conn.connect()
        conn.reconnectIntervalSeconds = 2

        def resp = conn.callRpc(new JsonRpcRequest("foo_bar", []))

        then:
        StepVerifier.create(resp)
            .then { server.stop() }
            .expectError()
            .verify(Duration.ofSeconds(1))
    }

    def "Sets DISCONNECTED status right after disconnect"() {
        setup:
        def up = Mock(DefaultUpstream) {
            _ * getId() >> "test"
        }
        WsConnection.ConnectionStatus result = null
        conn = new EthereumWsFactory("test", Chain.ETHEREUM, "ws://localhost:${port}".toURI(), "http://localhost:${port}".toURI())
                .create({ result = it  })
        when:
        conn.connect()
        conn.reconnectIntervalSeconds = 10
        Thread.sleep(SLEEP)
        server.stop()
        Thread.sleep(100)

        then:
        result == WsConnection.ConnectionStatus.DISCONNECTED
    }

    def "Try to connects to server until it's available"() {
        when:
        server.stop()
        Thread.sleep(SLEEP)
        conn.reconnectIntervalSeconds = 1
        conn.connect()
        Thread.sleep(3_000)
        server = new MockWSServer(port)
        server.start()
        server.onNextReply('{"jsonrpc":"2.0","id":100,"result":1}')
        Thread.sleep(3_000)

        def resp = conn.callRpc(new JsonRpcRequest("foo_bar", [])).block(Duration.ofSeconds(1))
        def act = server.received
        then:
        act.size() == 1
        act[0].value.contains("\"method\":\"foo_bar\"")
    }

    def "Call after reconnect"() {
        when:
        conn.connect()
        conn.reconnectIntervalSeconds = 2
        Thread.sleep(SLEEP)
        server.stop()
        Thread.sleep(SLEEP)
        server = new MockWSServer(port)
        server.start()
        // reconnects in 2 seconds, give 1 extra
        Thread.sleep(3_000)

        def resp = conn.callRpc(new JsonRpcRequest("foo_bar", []))
        then:
        StepVerifier.create(resp)
                .then {
                    server.onNextReply('{"jsonrpc":"2.0", "id":100, "result": "baz"}')
                }
                .expectNextMatches {
                    it.hasResult() && it.resultAsProcessedString == "baz"
                }
                .expectComplete()
                .verify(Duration.ofSeconds(5))
    }
}
