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

import spock.lang.Specification

class JsonRpcResponseSpec extends Specification {

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
}
