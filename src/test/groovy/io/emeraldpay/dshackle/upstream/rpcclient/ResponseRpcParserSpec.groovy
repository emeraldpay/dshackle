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

import io.emeraldpay.dshackle.upstream.ethereum.rpc.RpcResponseError
import spock.lang.Specification

class ResponseRpcParserSpec extends Specification {

    ResponseRpcParser parser = new ResponseRpcParser()

    def "Parse string response"() {
        setup:
        //          0       8       16                32  35
        def json = '{"jsonrpc": "2.0", "id": 1, "result": "Hello world!"}'
        when:
        def act = parser.parse(json.getBytes())
        then:
        act.error == null
        new String(act.result) == '"Hello world!"'
    }

    def "Parse string response when result starts first"() {
        setup:
        //          0       8       16                32  35
        def json = '{"result": "Hello world!", "jsonrpc": "2.0", "id": 1}'
        when:
        def act = parser.parse(json.getBytes())
        then:
        act.error == null
        new String(act.result) == '"Hello world!"'
    }

    def "Parse bool response"() {
        setup:
        //          0       8       16                32
        def json = '{"jsonrpc": "2.0", "id": 1, "result": false}'
        when:
        def act = parser.parse(json.getBytes())
        then:
        act.error == null
        new String(act.result) == 'false'
    }

    def "Parse bool response when id is last"() {
        setup:
        //          0       8       16                32
        def json = '{"jsonrpc": "2.0", "result": false, "id": 1}'
        when:
        def act = parser.parse(json.getBytes())
        then:
        act.error == null
        new String(act.result) == 'false'
    }

    def "Parse int response"() {
        setup:
        //          0       8       16                32
        def json = '{"jsonrpc": "2.0", "id": 1, "result": 100}'
        when:
        def act = parser.parse(json.getBytes())
        then:
        act.error == null
        new String(act.result) == '100'
    }

    def "Parse null response"() {
        setup:
        //          0       8       16                32
        def json = '{"jsonrpc": "2.0", "id": 1, "result": null}'
        when:
        def act = parser.parse(json.getBytes())
        then:
        act.error == null
        new String(act.result) == 'null'
    }

    def "Parse object response"() {
        setup:
        //          0       8       16                32
        def json = '{"jsonrpc": "2.0", "id": 1, "result": {"hash": "0x00000", "foo": false, "bar": 1}}'
        when:
        def act = parser.parse(json.getBytes())
        then:
        act.error == null
        new String(act.result) == '{"hash": "0x00000", "foo": false, "bar": 1}'
    }

    def "Parse object response with null error"() {
        setup:
        //          0       8       16                32
        def json = '{"jsonrpc": "2.0", "id": 1, "result": {"hash": "0x00000", "foo": false, "bar": 1}, "error": null}'
        when:
        def act = parser.parse(json.getBytes())
        then:
        act.error == null
        new String(act.result) == '{"hash": "0x00000", "foo": false, "bar": 1}'
    }

    def "Parse object response if null error comes first"() {
        setup:
        //          0       8       16                32
        def json = '{"jsonrpc": "2.0", "id": 1, "error": null, "result": {"hash": "0x00000", "foo": false, "bar": 1}}'
        when:
        def act = parser.parse(json.getBytes())
        then:
        act.error == null
        new String(act.result) == '{"hash": "0x00000", "foo": false, "bar": 1}'
    }

    def "Parse object response with extra spaces"() {
        setup:
        //          0       8       16                32
        def json = '{"jsonrpc": "2.0", "result"  :   {"hash": "0x00000", "foo": false , "bar":1}  , "id": 1}'
        when:
        def act = parser.parse(json.getBytes())
        then:
        act.error == null
        new String(act.result) == '{"hash": "0x00000", "foo": false , "bar":1}'
    }

    def "Parse complex object response"() {
        setup:
        //          0       8       16                32
        def json = '{"jsonrpc": "2.0", "id": 1, "result": {"hash": "0x00000", "foo": {"bar": 1, "baz": 2}}}'
        when:
        def act = parser.parse(json.getBytes())
        then:
        act.error == null
        new String(act.result) == '{"hash": "0x00000", "foo": {"bar": 1, "baz": 2}}'
    }

    def "Parse array response"() {
        setup:
        //          0       8       16                32
        def json = '{"jsonrpc": "2.0", "id": 1, "result": [1, 2, false]}'
        when:
        def act = parser.parse(json.getBytes())
        then:
        act.error == null
        new String(act.result) == '[1, 2, false]'
    }

    def "Parse error"() {
        setup:
        //          0       8       16                32
        def json = '{"jsonrpc": "2.0", "id": 1, "result": null, "error": {"code": -1111, "message": "test"}}'
        when:
        def act = parser.parse(json.getBytes())
        then:
        act.error != null
        act.error.code == -1111
        act.error.message == "test"
        act.hasError()
        !act.hasResult()
    }

    def "Parse error with no result field"() {
        setup:
        def json = '{"jsonrpc": "2.0", "id": 1, "error": {"code": -1111, "message": "test"}}'
        when:
        def act = parser.parse(json.getBytes())
        then:
        act.error != null
        act.error.code == -1111
        act.error.message == "test"
        act.hasError()
        !act.hasResult()
    }

    def "Parse error with data"() {
        setup:
        //          0       8       16                32
        def json = '{"jsonrpc": "2.0", "id": 1, "result": null, "error": {"code": -1111, "message": "test", "data": "just data"}}'
        when:
        def act = parser.parse(json.getBytes())
        then:
        act.error != null
        act.error.code == -1111
        act.error.message == "test"
        act.error.details == "just data"
        act.hasError()
        !act.hasResult()
    }

    def "Parse error with data struct"() {
        setup:
        //          0       8       16                32
        def json = '{"jsonrpc": "2.0", "id": 1, "result": null, "error": {"code": -1111, "message": "test", "data": {"foo": "just data", "bar": 1}}}'
        when:
        def act = parser.parse(json.getBytes())
        then:
        act.error != null
        act.error.code == -1111
        act.error.message == "test"
        act.error.details == [foo: "just data", bar: 1]
        act.hasError()
        !act.hasResult()
    }

    def "Handle non-json with producing an error response"() {
        setup:
        def json = 'NOT JSON'
        when:
        def act = parser.parse(json.getBytes())
        then:
        act.error != null
        act.error.code == RpcResponseError.CODE_UPSTREAM_INVALID_RESPONSE
        act.hasError()
        !act.hasResult()
    }

}
