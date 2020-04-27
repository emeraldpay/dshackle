package io.emeraldpay.dshackle.upstream.bitcoin

import io.emeraldpay.dshackle.test.TestingCommons
import io.infinitape.etherjar.rpc.RpcException
import org.mockserver.integration.ClientAndServer
import org.mockserver.model.HttpRequest
import org.mockserver.model.HttpResponse
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Duration

class DirectBitcoinApiSpec extends Specification {

    ClientAndServer mockServer
    DirectBitcoinApi api

    def setup() {
        mockServer = ClientAndServer.startClientAndServer(18332);
        api = new DirectBitcoinApi(
                new BitcoinRpcClient("localhost:18332", null),
                TestingCommons.objectMapper(), new DefaultBitcoinMethods(TestingCommons.objectMapper())
        )
    }

    def cleanup() {
        mockServer.stop()
    }

    def "Request simple"() {
        setup:
        def resp = '{' +
                '  "result": "0000000000000000000889c2e52ca5e1cecac60bce9a3754201a7a9a67791e90",' +
                '  "error": null,' +
                '  "id": 15' +
                '}'
        mockServer.when(
                HttpRequest.request()
        ).respond(
                HttpResponse.response(resp)
        )
        when:
        def act = api.executeAndResult(15, "getbestblockhash", [], String)
        then:
        StepVerifier.create(act)
                .expectNext("0000000000000000000889c2e52ca5e1cecac60bce9a3754201a7a9a67791e90")
                .expectComplete()
                .verify(Duration.ofSeconds(1))
        mockServer.verify(
                HttpRequest.request()
                        .withMethod("POST")
                        .withBody('{"jsonrpc":"2.0","method":"getbestblockhash","params":[],"id":15}')
        )
    }

    def "Request with params"() {
        setup:
        def resp = '{' +
                '  "result": "something",' +
                '  "id": 1' +
                '}'
        mockServer.when(
                HttpRequest.request()
        ).respond(
                HttpResponse.response(resp)
        )
        when:
        def act = api.executeAndResult(1, "getsomething", ["something", false], String)
        then:
        StepVerifier.create(act)
                .expectNext("something")
                .expectComplete()
                .verify(Duration.ofSeconds(1))
        mockServer.verify(
                HttpRequest.request()
                        .withMethod("POST")
                        .withBody('{"jsonrpc":"2.0","method":"getsomething","params":["something",false],"id":1}')
        )
    }

    def "Returns error"() {
        setup:
        def resp = '{' +
                '  "result": null,' +
                '  "error": {' +
                '    "code": -32601,' +
                '    "message": "Method not found"' +
                '  },' +
                '  "id": 1' +
                '}'
        mockServer.when(
                HttpRequest.request()
        ).respond(
                HttpResponse.response(resp)
        )
        when:
        def act = api.executeAndResult(1, "geterror", [], String)
        then:
        StepVerifier.create(act)
                .expectError(RpcException)
                .verify(Duration.ofSeconds(1))
        mockServer.verify(
                HttpRequest.request()
                        .withMethod("POST")
                        .withBody('{"jsonrpc":"2.0","method":"geterror","params":[],"id":1}')
        )
    }
}
