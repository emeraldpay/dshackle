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
import io.emeraldpay.dshackle.upstream.ChainCallError
import io.emeraldpay.dshackle.upstream.ChainResponse
import spock.lang.Specification

class JsonRpcResponseSpec extends Specification {

    ObjectMapper objectMapper = Global.objectMapper

    def "Same responses are equal"() {
        setup:
        def resp1 = new ChainResponse("\"hello\"".bytes, null)
        def resp2 = new ChainResponse("\"hello\"".bytes, null)
        when:
        def act = resp1.equals(resp2)
        then:
        act
    }

    def "Extract processed string without quoted"() {
        when:
        def act = new ChainResponse("\"hello\"".bytes, null).resultAsProcessedString
        then:
        act == "hello"
    }

    def "Extract raw string with quoted"() {
        when:
        def act = new ChainResponse("\"hello\"".bytes, null).resultAsRawString
        then:
        act == "\"hello\""
    }

    def "Fails to extract processed string if not quoted"() {
        when:
        new ChainResponse("{\"hello\": 1}".bytes, null).resultAsProcessedString
        then:
        thrown(IllegalStateException)
    }

    def "Recognizes null"() {
        when:
        def act = new ChainResponse("null".bytes, null)
        then:
        act.isNull()
    }

    def "Serialize int id and null result"() {
        setup:
        def json = new ChainResponse("null".bytes, null, new ChainResponse.NumberId(1), null, null, null)
        when:
        def act = objectMapper.writeValueAsString(json)
        then:
        act == '{"jsonrpc":"2.0","id":1,"result":null}'
    }

    def "Serialize int id and string result"() {
        setup:
        def json = new ChainResponse('"Hello World"'.bytes, null, new ChainResponse.NumberId(10), null, null, null)
        when:
        def act = objectMapper.writeValueAsString(json)
        then:
        act == '{"jsonrpc":"2.0","id":10,"result":"Hello World"}'
    }

    def "Serialize int id and object result"() {
        setup:
        def json = new ChainResponse('{"foo": "Hello World", "bar": 1}'.bytes, null, new ChainResponse.NumberId(101), null, null, null)
        when:
        def act = objectMapper.writeValueAsString(json)
        then:
        act == '{"jsonrpc":"2.0","id":101,"result":{"foo": "Hello World", "bar": 1}}'
    }

    def "Serialize int id and error"() {
        setup:
        def json = new ChainResponse(null, new ChainCallError(-32041, "Oooops"), new ChainResponse.NumberId(101), null, null, null)
        when:
        def act = objectMapper.writeValueAsString(json)
        then:
        act == '{"jsonrpc":"2.0","id":101,"error":{"code":-32041,"message":"Oooops"}}'
    }

    def "Serialize string id and null result"() {
        setup:
        def json = new ChainResponse("null".bytes, null, new ChainResponse.StringId("asf01t1gg"), null, null, null)
        when:
        def act = objectMapper.writeValueAsString(json)
        then:
        act == '{"jsonrpc":"2.0","id":"asf01t1gg","result":null}'
    }

    def "Serialize string id and string result"() {
        setup:
        def json = new ChainResponse('"Hello World"'.bytes, null, new ChainResponse.StringId("10"), null, null, null)
        when:
        def act = objectMapper.writeValueAsString(json)
        then:
        act == '{"jsonrpc":"2.0","id":"10","result":"Hello World"}'
    }

    def "Serialize string id and object result"() {
        setup:
        def json = new ChainResponse('{"foo": "Hello World", "bar": 1}'.bytes, null, new ChainResponse.StringId("g8gk19g"), null, null, null)
        when:
        def act = objectMapper.writeValueAsString(json)
        then:
        act == '{"jsonrpc":"2.0","id":"g8gk19g","result":{"foo": "Hello World", "bar": 1}}'
    }

    def "Serialize string id and error"() {
        setup:
        def json = new ChainResponse(null,
                new ChainCallError(-32041, "Oooops"),
                new ChainResponse.StringId("9kbo29gkaasf"), null, null, null  )
        when:
        def act = objectMapper.writeValueAsString(json)
        then:
        act == '{"jsonrpc":"2.0","id":"9kbo29gkaasf","error":{"code":-32041,"message":"Oooops"}}'
    }
}
