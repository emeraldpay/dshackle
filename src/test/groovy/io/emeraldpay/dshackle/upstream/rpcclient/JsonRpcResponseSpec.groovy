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

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.Global
import spock.lang.Specification

class JsonRpcResponseSpec extends Specification {

    ObjectMapper objectMapper = Global.objectMapper

    def "Same responses are equal"() {
        setup:
        def resp1 = new JsonRpcResponse("\"hello\"".bytes, null)
        def resp2 = new JsonRpcResponse("\"hello\"".bytes, null)
        when:
        def act = resp1.equals(resp2)
        then:
        act == true
    }

    def "Extract processed string without quoted"() {
        when:
        def act = new JsonRpcResponse("\"hello\"".bytes, null).resultAsProcessedString
        then:
        act == "hello"
    }

    def "Extract raw string with quoted"() {
        when:
        def act = new JsonRpcResponse("\"hello\"".bytes, null).resultAsRawString
        then:
        act == "\"hello\""
    }

    def "Fails to extract processed string if not quoted"() {
        when:
        def act = new JsonRpcResponse("{\"hello\": 1}".bytes, null).resultAsProcessedString
        then:
        thrown(IllegalStateException)
    }

    def "Recognizes null"() {
        when:
        def act = new JsonRpcResponse("null".bytes, null)
        then:
        act.isNull()
    }

    def "Serialize int id and null result"() {
        setup:
        def json = new JsonRpcResponse("null".bytes, null, new JsonRpcResponse.IntId(1))
        when:
        def act = objectMapper.writeValueAsString(json)
        then:
        act == '{"jsonrpc":"2.0","id":1,"result":null}'
    }

    def "Serialize int id and string result"() {
        setup:
        def json = new JsonRpcResponse('"Hello World"'.bytes, null, new JsonRpcResponse.IntId(10))
        when:
        def act = objectMapper.writeValueAsString(json)
        then:
        act == '{"jsonrpc":"2.0","id":10,"result":"Hello World"}'
    }

    def "Serialize int id and object result"() {
        setup:
        def json = new JsonRpcResponse('{"foo": "Hello World", "bar": 1}'.bytes, null, new JsonRpcResponse.IntId(101))
        when:
        def act = objectMapper.writeValueAsString(json)
        then:
        act == '{"jsonrpc":"2.0","id":101,"result":{"foo": "Hello World", "bar": 1}}'
    }

    def "Serialize int id and error"() {
        setup:
        def json = new JsonRpcResponse(null, new JsonRpcError(-32041, "Oooops"), new JsonRpcResponse.IntId(101))
        when:
        def act = objectMapper.writeValueAsString(json)
        then:
        act == '{"jsonrpc":"2.0","id":101,"error":{"code":-32041,"message":"Oooops"}}'
    }

    def "Serialize string id and null result"() {
        setup:
        def json = new JsonRpcResponse("null".bytes, null, new JsonRpcResponse.StringId("asf01t1gg"))
        when:
        def act = objectMapper.writeValueAsString(json)
        then:
        act == '{"jsonrpc":"2.0","id":"asf01t1gg","result":null}'
    }

    def "Serialize string id and string result"() {
        setup:
        def json = new JsonRpcResponse('"Hello World"'.bytes, null, new JsonRpcResponse.StringId("10"))
        when:
        def act = objectMapper.writeValueAsString(json)
        then:
        act == '{"jsonrpc":"2.0","id":"10","result":"Hello World"}'
    }

    def "Serialize string id and object result"() {
        setup:
        def json = new JsonRpcResponse('{"foo": "Hello World", "bar": 1}'.bytes, null, new JsonRpcResponse.StringId("g8gk19g"))
        when:
        def act = objectMapper.writeValueAsString(json)
        then:
        act == '{"jsonrpc":"2.0","id":"g8gk19g","result":{"foo": "Hello World", "bar": 1}}'
    }

    def "Serialize string id and error"() {
        setup:
        def json = new JsonRpcResponse(null,
                new JsonRpcError(-32041, "Oooops"),
                new JsonRpcResponse.StringId("9kbo29gkaasf"))
        when:
        def act = objectMapper.writeValueAsString(json)
        then:
        act == '{"jsonrpc":"2.0","id":"9kbo29gkaasf","error":{"code":-32041,"message":"Oooops"}}'
    }
}
