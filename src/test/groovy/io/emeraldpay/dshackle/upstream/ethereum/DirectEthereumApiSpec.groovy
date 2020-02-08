/**
 * Copyright (c) 2019 ETCDEV GmbH
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
package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.upstream.DirectCallMethods
import io.infinitape.etherjar.rpc.ReactorRpcClient
import io.infinitape.etherjar.rpc.RpcException
import io.infinitape.etherjar.rpc.RpcResponseError
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.json.TransactionJson
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Duration

class DirectEthereumApiSpec extends Specification {

    DirectEthereumApi api = new DirectEthereumApi(Stub(ReactorRpcClient), null, TestingCommons.objectMapper(), new DirectCallMethods())

    def "Process successful result"() {
        setup:
        def result = Mono.just("hello")
        when:
        def act = api.processResult(1, "eth_test", result)
            .map { new String(it) }

        then:
        StepVerifier.create(act)
            .expectNext('{"jsonrpc":"2.0","id":1,"result":"hello"}')
            .expectComplete()
            .verify(Duration.ofSeconds(1))

    }

    def "Process empty result"() {
        setup:
        def result = Mono.empty()
        when:
        def act = api.processResult(1, "eth_test", result)
                .map { new String(it) }

        then:
        StepVerifier.create(act)
                .expectNext('{"jsonrpc":"2.0","id":1,"result":null}')
                .expectComplete()
                .verify(Duration.ofSeconds(1))

    }

    def "Process standard RPC error"() {
        setup:
        def result = Mono.error(new RpcException(RpcResponseError.CODE_METHOD_NOT_EXIST, "Test Error", Map.of("foo", "bar")))
        when:
        def act = api.processResult(1, "eth_test", result)
                .map { new String(it) }

        then:
        StepVerifier.create(act)
                .expectNext('{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"Test Error","data":{"foo":"bar"}}}')
                .expectComplete()
                .verify(Duration.ofSeconds(1))

    }

    def "Process internal exception"() {
        setup:
        def result = Mono.error(new InterruptedException("test"))
        when:
        def act = api.processResult(1, "eth_test", result)
                .map { new String(it) }

        then:
        StepVerifier.create(act)
                .expectNext('{"jsonrpc":"2.0","id":1,"error":{"code":-32020,"message":"Error reading from upstream"}}')
                .expectComplete()
                .verify(Duration.ofSeconds(1))

    }

    def "Typed mapping for block request"() {
        when:
        def act = api.callMapping("eth_getBlockByHash", ["0xacf5611707048efc39cabed483e420672ca1ed070f248ef6202c99994dbc6061", false])
        then:
        act.jsonType == BlockJson
        act.resultType == BlockJson
    }

    def "Typed mapping for block request with txes"() {
        when:
        def act = api.callMapping("eth_getBlockByHash", ["0xacf5611707048efc39cabed483e420672ca1ed070f248ef6202c99994dbc6061", true])
        then:
        act.jsonType == BlockJson
        act.resultType == BlockJson
    }

    def "Typed mapping for block by height request"() {
        when:
        def act = api.callMapping("eth_getBlockByNumber", ["0x135", false])
        then:
        act.jsonType == BlockJson
        act.resultType == BlockJson
    }

    def "Typed mapping for block by height request with txes"() {
        when:
        def act = api.callMapping("eth_getBlockByNumber", ["0xacf5", true])
        then:
        act.jsonType == BlockJson
        act.resultType == BlockJson
    }

    def "Typed mapping for tx request"() {
        when:
        def act = api.callMapping("eth_getTransactionByHash", ["0xacf5611707048efc39cabed483e420672ca1ed070f248ef6202c99994dbc6061"])
        then:
        act.jsonType == TransactionJson
        act.resultType == TransactionJson
    }

    def "Errors for mapping of invalid tx request"() {
        when:
        api.callMapping("eth_getTransactionByHash", ["0xacf5611707048efc39cabed483e420672ca1ed070f248ef6"])
        then:
        def t = thrown(RpcException)
        t.code == RpcResponseError.CODE_INVALID_METHOD_PARAMS

        when:
        api.callMapping("eth_getTransactionByHash", ["0xacf5611707048efc39cabed483e420672ca1ed070f248ef6202c99994dbc6061", true])
        then:
        t = thrown(RpcException)
        t.code == RpcResponseError.CODE_INVALID_METHOD_PARAMS

        when:
        api.callMapping("eth_getTransactionByHash", [])
        then:
        t = thrown(RpcException)
        t.code == RpcResponseError.CODE_INVALID_METHOD_PARAMS
    }

    def "Errors for mapping of invalid block request"() {
        when:
        api.callMapping("eth_getBlockByHash", ["0xacf5611707048efc39cabed483e420672ca1ed070f248ef6202c99994dbc6061"])
        then:
        def t = thrown(RpcException)
        t.code == RpcResponseError.CODE_INVALID_METHOD_PARAMS

        when:
        api.callMapping("eth_getBlockByHash", ["0xacf5611707048efc39cabed48f6202c99994dbc6061", true])
        then:
        t = thrown(RpcException)
        t.code == RpcResponseError.CODE_INVALID_METHOD_PARAMS

        when:
        api.callMapping("eth_getBlockByHash", [])
        then:
        t = thrown(RpcException)
        t.code == RpcResponseError.CODE_INVALID_METHOD_PARAMS
    }

    def "Errors for mapping of invalid block by number request"() {
        when:
        api.callMapping("eth_getBlockByNumber", ["0xacf5611707048efc3248ef6202c99994dbc6061"])
        then:
        def t = thrown(RpcException)
        t.code == RpcResponseError.CODE_INVALID_METHOD_PARAMS

        when:
        api.callMapping("eth_getBlockByNumber", ["0x", true])
        then:
        t = thrown(RpcException)
        t.code == RpcResponseError.CODE_INVALID_METHOD_PARAMS

        when:
        api.callMapping("eth_getBlockByNumber", ["-0x23", true])
        then:
        t = thrown(RpcException)
        t.code == RpcResponseError.CODE_INVALID_METHOD_PARAMS

        when:
        api.callMapping("eth_getBlockByNumber", [])
        then:
        t = thrown(RpcException)
        t.code == RpcResponseError.CODE_INVALID_METHOD_PARAMS
    }
}
