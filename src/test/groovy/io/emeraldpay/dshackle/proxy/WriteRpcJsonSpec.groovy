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

import io.emeraldpay.dshackle.rpc.NativeCall
import org.jetbrains.annotations.NotNull
import reactor.core.publisher.Flux
import spock.lang.Specification

import java.time.Duration

class WriteRpcJsonSpec extends Specification {

    WriteRpcJson writer = new WriteRpcJson()

    def "Write empty array"() {
        when:
        def act = Flux.empty().transform(writer.asArray())
                .collectList()
                .block(Duration.ofSeconds(1))
                .join("")
        then:
        act == "[]"
    }

    def "Write single item array"() {
        when:
        def act = Flux.just('{"id": 1}').transform(writer.asArray())
                .collectList()
                .block(Duration.ofSeconds(1))
                .join("")
        then:
        act == '[{"id": 1}]'
    }

    def "Write two item array"() {
        setup:
        def data = [
                '{"id": 1}',
                '{"id": 2}',
        ]
        when:
        def act = Flux.fromIterable(data).transform(writer.asArray())
                .collectList()
                .block(Duration.ofSeconds(1))
                .join("")
        then:
        act == '[{"id": 1},{"id": 2}]'
    }

    def "Write few items array"() {
        setup:
        def data = [
                '{"id": 1}',
                '{"id": 2, "foo": "bar"}',
                '{"id": 3, "foo": "baz"}',
                '{"id": 4}',
                '{"id": 5, "x": 5}',
        ]
        when:
        def act = Flux.fromIterable(data).transform(writer.asArray())
                .collectList()
                .block(Duration.ofSeconds(1))
                .join("")
        then:
        act == '[{"id": 1},{"id": 2, "foo": "bar"},{"id": 3, "foo": "baz"},{"id": 4},{"id": 5, "x": 5}]'
    }

    def "Convert basic to JSON"() {
        setup:
        def call = new ProxyCall(ProxyCall.RpcType.SINGLE)
        call.ids[1] = 105
        def data = [
                new NativeCall.CallResult(1, null, '"0x98dbb1"'.bytes, null, null)
        ]
        when:
        def act = writer.toJson(call, data[0])
        then:
        act == '{"jsonrpc":"2.0","id":105,"result":"0x98dbb1"}'
    }

    def "Convert gRPC error to JSON"() {
        setup:
        def call = new ProxyCall(ProxyCall.RpcType.SINGLE)
        call.ids[1] = 1
        def data = [
                new NativeCall.CallResult(1, null, null, new NativeCall.CallError(1, "Internal Error", null), null)
        ]
        when:
        def act = writer.toJson(call, data[0])
        then:
        act == '{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"Internal Error"}}'
    }

    def "Convert basic to JSON with string id"() {
        setup:
        def call = new ProxyCall(ProxyCall.RpcType.SINGLE)
        call.ids[1] = "aaa"
        def data = [
                new NativeCall.CallResult(1, null, '"0x98dbb1"'.bytes, null, null)
        ]
        when:
        def act = writer.toJson(call, data[0])
        then:
        act == '{"jsonrpc":"2.0","id":"aaa","result":"0x98dbb1"}'
    }

    def "Convert few items to JSON"() {
        setup:
        def call = new ProxyCall(ProxyCall.RpcType.SINGLE)
        call.ids[1] = 10
        call.ids[2] = 11
        call.ids[3] = 15
        def data = [
                new NativeCall.CallResult(1, null, '"0x98dbb1"'.bytes, null, null),
                new NativeCall.CallResult(2, null, null, new NativeCall.CallError(2, "oops", null), null),
                new NativeCall.CallResult(3, null, '{"hash": "0x2484f459dc"}'.bytes, null, null),
        ]
        when:
        def act = Flux.fromIterable(data)
                .transform(writer.toJsons(call))
                .collectList()
                .block(Duration.ofSeconds(1))
        then:
        act.size() == 3
        act[0] == '{"jsonrpc":"2.0","id":10,"result":"0x98dbb1"}'
        act[1] == '{"jsonrpc":"2.0","id":11,"error":{"code":-32603,"message":"oops"}}'
        act[2] == '{"jsonrpc":"2.0","id":15,"result":{"hash": "0x2484f459dc"}}'
    }

    def "Write JSON RPC error on exception"() {
        setup:
        def writer = new WriteRpcJson() {
            @Override
            String toJson(@NotNull ProxyCall call, @NotNull NativeCall.CallResult response) {
                throw new NativeCall.CallFailure(1, new IllegalStateException("TEST"))
            }
        }

        def call = new ProxyCall(ProxyCall.RpcType.SINGLE)
        call.ids[1] = 10
        def data = [
                new NativeCall.CallResult(1, null, '"0x1"'.bytes, null, null),
        ]
        when:
        def act = Flux.fromIterable(data)
                .transform(writer.toJsons(call))
                .collectList()
                .block(Duration.ofSeconds(1))
        then:
        act[0] == '{"jsonrpc":"2.0","id":10,"error":{"code":-32603,"message":"TEST"}}'
    }
}
