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
import org.mockserver.integration.ClientAndServer
import org.mockserver.model.HttpRequest
import org.mockserver.model.HttpResponse
import spock.lang.Specification

class JsonRpcClientSpec extends Specification {

    ClientAndServer mockServer
    JsonRpcClient client

    def setup() {
        mockServer = ClientAndServer.startClientAndServer(18332);
        client = new JsonRpcClient("localhost:18332", TestingCommons.objectMapper(), null)
    }

    def cleanup() {
        mockServer.stop()
    }


    def "Make a request"() {
        setup:
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
        def act = client.execute(new JsonRpcRequest("test", [])).block()
        then:
        act.error == null
        new String(act.result) == '"0x98de45"'
    }

}
