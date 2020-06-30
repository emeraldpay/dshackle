/**
 * Copyright (c) 2020 ETCDEV GmbH
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
package io.emeraldpay.dshackle.proxy

import io.emeraldpay.dshackle.test.TestingCommons
import io.infinitape.etherjar.rpc.RpcException
import spock.lang.Specification

class ReadRpcJsonSpec extends Specification {

    ReadRpcJson reader = new ReadRpcJson()

    def "Get first symbol"() {
        expect:
        reader.getStartOfJson(input.bytes) == exp.bytes[0]
        where:
        exp | input
        "{" | "{}"
        "{" | "   { }"
        "{" | "\n\n { }"
        "[" | " [ { } ] "
    }

    def "Error for input with many spaces"() {
        setup:
        def empty = " " * 1000
        when:
        reader.getStartOfJson((empty + "{}").bytes)
        then:
        thrown(IllegalArgumentException)
    }

    def "Error for empty spaces"() {
        when:
        reader.getStartOfJson("".bytes)
        then:
        thrown(IllegalArgumentException)
    }

    def "Get type"() {
        expect:
        reader.getType(input.bytes) == exp
        where:
        exp                      | input
        ProxyCall.RpcType.SINGLE | "{}"
        ProxyCall.RpcType.SINGLE | "   { }"
        ProxyCall.RpcType.SINGLE | "\n\n { }"
        ProxyCall.RpcType.BATCH  | " [ { } ] "
    }

    def "Error type for invalid input"() {
        when:
        reader.getType("hello".bytes)
        then:
        thrown(RpcException)

        when:
        reader.getType("1".bytes)
        then:
        thrown(RpcException)

        when:
        reader.getType("".bytes)
        then:
        thrown(RpcException)
    }

    def "Parse basic"() {
        when:
        def act = reader.apply('{"jsonrpc":"2.0", "method":"net_peerCount", "id":1, "params":[]}'.bytes)
        then:
        act.type == ProxyCall.RpcType.SINGLE
        act.ids.size() == 1
        act.ids[0] == 1
        act.items.size() == 1
        with(act.items[0]) {
            id == 0
            method == "net_peerCount"
            payload.toStringUtf8() == "[]"
        }
    }

    def "Parse basic with string id"() {
        when:
        def act = reader.apply('{"jsonrpc":"2.0", "method":"net_peerCount", "id":"ggk19K5a", "params":[]}'.bytes)
        then:
        act.type == ProxyCall.RpcType.SINGLE
        act.ids.size() == 1
        act.ids[0] == "ggk19K5a"
        act.items.size() == 1
        with(act.items[0]) {
            id == 0
            method == "net_peerCount"
            payload.toStringUtf8() == "[]"
        }
    }

    def "Parse basic without params"() {
        when:
        def act = reader.apply('{"jsonrpc":"2.0", "method":"net_peerCount", "id":2}'.bytes)
        then:
        act.type == ProxyCall.RpcType.SINGLE
        act.ids.size() == 1
        act.ids[0] == 2
        act.items.size() == 1
        with(act.items[0]) {
            id == 0
            method == "net_peerCount"
            payload.toStringUtf8() == "[]"
        }
    }

    def "Parse with parameters"() {
        when:
        def act = reader.apply('{"jsonrpc":"2.0", "method":"net_peerCount", "id":1, "params":["0x015f"]}'.bytes)
        then:
        act.type == ProxyCall.RpcType.SINGLE
        act.ids.size() == 1
        act.ids[0] == 1
        act.items.size() == 1
        with(act.items[0]) {
            id == 0
            method == "net_peerCount"
            payload.toStringUtf8() == '["0x015f"]'
        }
    }

    def "Parse single batch"() {
        when:
        def act = reader.apply('[{"jsonrpc":"2.0", "method":"net_peerCount", "id":1, "params":[]}]'.bytes)
        then:
        act.type == ProxyCall.RpcType.BATCH
        act.ids.size() == 1
        act.ids[0] == 1
        act.items.size() == 1
        with(act.items[0]) {
            id == 0
            method == "net_peerCount"
            payload.toStringUtf8() == "[]"
        }
    }

    def "Parse multi batch"() {
        when:
        def act = reader.apply('[{"jsonrpc":"2.0", "method":"net_peerCount", "id":"xdd", "params":[]}, {"jsonrpc":"2.0", "method":"foo_bar", "id":4, "params":[143, false]}]'.bytes)
        then:
        act.type == ProxyCall.RpcType.BATCH
        act.ids.size() == 2
        act.ids[0] == "xdd"
        act.ids[1] == 4
        act.items.size() == 2
        with(act.items[0]) {
            id == 0
            method == "net_peerCount"
            payload.toStringUtf8() == "[]"
        }
        with(act.items[1]) {
            id == 1
            method == "foo_bar"
            payload.toStringUtf8() == '[143,false]'
        }
    }
}
