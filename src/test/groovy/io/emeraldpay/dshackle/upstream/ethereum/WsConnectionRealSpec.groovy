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

class WsConnectionRealSpec extends Specification {

    static SLEEP = 500

    static int port = 19900 + new Random().nextInt(100)
    @Shared
    MockWSServer server
    @Shared
    WsConnection conn

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
        conn = new EthereumWsFactory("test", Chain.ETHEREUM, "ws://localhost:${port}".toURI(), "http://localhost:${port}".toURI()).create(null, null)
    }

    def cleanup() {
        conn.close()
        server.stop()
    }

    def "Connects to server"() {
        when:
        conn.connect()
        Thread.sleep(SLEEP)
        println("verify....")
        def act = server.received
        then:
        act.size() > 0
        act[0].value.contains("\"method\":\"eth_subscribe\"")
        act[0].value.contains("\"params\":[\"newHeads\"]")
    }

    def "Makes RPC request"() {
        when:
        conn.connect()
        def resp = conn.call(new JsonRpcRequest("foo_bar", []))
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
        act.size() == 2
        act[1].value.contains("\"method\":\"foo_bar\"")
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
        def resp = conn.call(new JsonRpcRequest("foo_bar", []))
        // reconnects in 2 seconds, give 1 extra
        Thread.sleep(3_000)
        def act = server.received

        then:
        act.size() > 0
        act[0].value.contains("\"method\":\"eth_subscribe\"")
        act[0].value.contains("\"params\":[\"newHeads\"]")
    }

    def "Error on request when server disconnects"() {
        when:
        conn.connect()
        conn.reconnectIntervalSeconds = 2

        def resp = conn.call(new JsonRpcRequest("foo_bar", []))

        then:
        StepVerifier.create(resp)
            .then { server.stop() }
            .expectError()
            .verify(Duration.ofSeconds(1))
    }

    def "Gets UNAVAIL status right after disconnect"() {
        setup:
        def up = Mock(DefaultUpstream) {
            _ * getId() >> "test"
        }
        conn = new EthereumWsFactory("test", Chain.ETHEREUM, "ws://localhost:${port}".toURI(), "http://localhost:${port}".toURI()).create(up, null)
        when:
        conn.connect()
        conn.reconnectIntervalSeconds = 10
        Thread.sleep(SLEEP)
        server.stop()
        Thread.sleep(100)

        then:
        1 * up.setStatus(UpstreamAvailability.UNAVAILABLE)
    }

    def "Validates after connect"() {
        setup:
        def validator = Mock(EthereumUpstreamValidator)
        conn = new EthereumWsFactory("test", Chain.ETHEREUM, "ws://localhost:${port}".toURI(), "http://localhost:${port}".toURI()).create(null, validator)
        when:
        conn.connect()
        Thread.sleep(100)

        then:
        1 * validator.validate()
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
        Thread.sleep(2_000)
        def act = server.received
        then:
        act.size() > 0
        act[0].value.contains("\"method\":\"eth_subscribe\"")
        act[0].value.contains("\"params\":[\"newHeads\"]")
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

        def resp = conn.call(new JsonRpcRequest("foo_bar", []))
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
