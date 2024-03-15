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


import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.upstream.ChainException
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.RequestMetrics
import io.emeraldpay.dshackle.upstream.ethereum.rpc.RpcResponseError
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Timer
import org.mockserver.integration.ClientAndServer
import org.mockserver.model.HttpRequest
import org.mockserver.model.HttpResponse
import org.springframework.util.SocketUtils
import spock.lang.Specification

import java.time.Duration

class JsonRpcHttpReaderSpec extends Specification {

    ClientAndServer mockServer
    int port = 19332
    RequestMetrics metrics = new RequestMetrics(
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
        JsonRpcHttpReader client = new JsonRpcHttpReader("localhost:${port}", metrics,null, null)
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
        def act = client.read(new ChainRequest("test", new ListParams())).block()
        then:
        act.error == null
        new String(act.result) == '"0x98de45"'
    }

    def "Produces RPC Exception on error status code"() {
        setup:
        def client = new JsonRpcHttpReader("localhost:${port}", metrics, null, null)

        mockServer.when(
                HttpRequest.request()
        ).respond(
                HttpResponse.response()
                        .withStatusCode(500)
                        .withBody("pong")
        )
        when:
        def act = client.read(
                new ChainRequest("ping", new ListParams())
        ).block(Duration.ofSeconds(1))
        then:
        def t = thrown(RuntimeException) // reactor.core.Exceptions$ReactiveException
        t.cause instanceof ChainException
        with(((ChainException)t.cause).error) {
            code == RpcResponseError.CODE_UPSTREAM_INVALID_RESPONSE
            message == "HTTP Code: 500"
        }
    }

    def "Tries to extract message if HTTP error if it still contains a JSON RPC message"() {
        setup:
        def client = new JsonRpcHttpReader("localhost:${port}", metrics, null, null)

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
                new ChainRequest("ping", new ListParams())
        ).block(Duration.ofSeconds(1))
        then:
        def t = thrown(RuntimeException) // reactor.core.Exceptions$ReactiveException
        t.cause instanceof ChainException
        with(((ChainException)t.cause).error) {
            code == -32603
            message == "Something happened"
        }
    }

}
