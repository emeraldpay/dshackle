/**
 * Copyright (c) 2020 EmeraldPay, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.emeraldpay.dshackle.upstream.rpcclient

import io.emeraldpay.dshackle.config.AuthConfig
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.etherjar.rpc.RpcResponseError
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Timer
import org.mockserver.integration.ClientAndServer
import org.mockserver.model.HttpRequest
import org.mockserver.model.HttpResponse
import org.mockserver.model.MediaType
import org.springframework.util.SocketUtils
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Duration

class JsonRpcHttpClientSpec extends Specification {

    ClientAndServer mockServer
    int port = 19332
    RpcMetrics metrics = new RpcMetrics(
            Timer.builder("test1").register(TestingCommons.meterRegistry),
            Counter.builder("test2").register(TestingCommons.meterRegistry)
    )

    def setup() {
        port = SocketUtils.findAvailableTcpPort(19332)
        mockServer = ClientAndServer.startClientAndServer(port);
    }

    def cleanup() {
        mockServer.stop()
    }

    def "Make a request"() {
        setup:
        JsonRpcHttpClient client = new JsonRpcHttpClient("localhost:${port}", metrics,null, null)
        def resp = '{' +
                '  "jsonrpc": "2.0",' +
                '  "result": "0x98de45",' +
                '  "error": null,' +
                '  "id": 15' +
                '}'
        mockServer.when(
                HttpRequest.request()
        ).respond(
                HttpResponse.response(resp)
        )
        when:
        def act = client.read(new JsonRpcRequest("test", [])).block()
        then:
        act.error == null
        new String(act.result) == '"0x98de45"'
    }

    def "Make request with basic auth"() {
        setup:
        def auth = new AuthConfig.ClientBasicAuth("user", "passwd")
        def client = new JsonRpcHttpClient("localhost:${port}", metrics, auth, null)

        mockServer.when(
                HttpRequest.request()
                        .withMethod("POST")
                        .withBody("ping")
        ).respond(
                HttpResponse.response()
                        .withBody("pong")
        )
        when:
        def act = client.execute("ping".bytes).map { new String(it.t2) }
        then:
        StepVerifier.create(act)
                .expectNext("pong")
                .expectComplete()
                .verify(Duration.ofSeconds(1))
        mockServer.verify(
                HttpRequest.request()
                        .withMethod("POST")
                        .withBody("ping")
                        .withContentType(MediaType.APPLICATION_JSON)
                        .withHeader("authorization", "Basic dXNlcjpwYXNzd2Q=")
        )
    }

    def "Produces RPC Exception on error status code"() {
        setup:
        def client = new JsonRpcHttpClient("localhost:${port}", metrics, null, null)

        mockServer.when(
                HttpRequest.request()
        ).respond(
                HttpResponse.response()
                        .withStatusCode(500)
                        .withBody("pong")
        )
        when:
        def act = client.read(
                new JsonRpcRequest("ping", [])
        ).block(Duration.ofSeconds(1))
        then:
        def t = thrown(RuntimeException) // reactor.core.Exceptions$ReactiveException
        t.cause instanceof JsonRpcException
        with(((JsonRpcException)t.cause).error) {
            code == RpcResponseError.CODE_UPSTREAM_INVALID_RESPONSE
            message == "HTTP Code: 500"
        }
    }

    def "Tries to extract message if HTTP error if it still contains a JSON RPC message"() {
        setup:
        def client = new JsonRpcHttpClient("localhost:${port}", metrics, null, null)

        mockServer.when(
                HttpRequest.request()
        ).respond(
                HttpResponse.response()
                        .withStatusCode(500)
                        .withBody('{' +
                                '"jsonrpc": "2.0", ' +
                                '"id": 1, ' +
                                '"error": {"code": -32603, "message": "Something happened"}' +
                                '}')
        )
        when:
        def act = client.read(
                new JsonRpcRequest("ping", [])
        ).block(Duration.ofSeconds(1))
        then:
        def t = thrown(RuntimeException) // reactor.core.Exceptions$ReactiveException
        t.cause instanceof JsonRpcException
        with(((JsonRpcException)t.cause).error) {
            code == -32603
            message == "Something happened"
        }
    }

}
