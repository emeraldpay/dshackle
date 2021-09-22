package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.test.MockWSServer
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import reactor.test.StepVerifier
import spock.lang.Shared
import spock.lang.Specification

import java.time.Duration

class EthereumWsFactoryRealSpec extends Specification {

    // needs large timeouts and sleep, especially on CI where it's much slower to run
    static TIMEOUT = 15
    static SLEEP = 500

    static int port = 19900 + new Random().nextInt(100)
    @Shared
    MockWSServer server
    @Shared
    EthereumWsFactory.EthereumWs conn

    def setup() {
        port++
        server = new MockWSServer(port)
        server.start()
        Thread.sleep(SLEEP)
        conn = new EthereumWsFactory("ws://localhost:${port}".toURI(), "http://localhost:${port}".toURI()).create(null)
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
                    server.reply('{"jsonrpc":"2.0", "id":100, "result": "baz"}')
                }
                .expectNextMatches {
                    it.hasResult() && it.resultAsProcessedString == "baz"
                }
                .expectComplete()
                .verify(Duration.ofSeconds(3))

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
        conn.retryInterval = 2
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

    def "Try to connects to server until it's available"() {
        when:
        server.stop()
        Thread.sleep(SLEEP)
        conn.retryInterval = 1
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
        conn.retryInterval = 2
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
                    server.reply('{"jsonrpc":"2.0", "id":100, "result": "baz"}')
                }
                .expectNextMatches {
                    it.hasResult() && it.resultAsProcessedString == "baz"
                }
                .expectComplete()
                .verify(Duration.ofSeconds(3))
    }
}
