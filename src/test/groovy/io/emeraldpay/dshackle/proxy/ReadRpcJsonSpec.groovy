/**
 * Copyright (c) 2020 ETCDEV GmbH
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

    ReadRpcJson reader = new ReadRpcJson(TestingCommons.objectMapper())

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
}
