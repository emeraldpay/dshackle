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

class JsonRpcRequestSpec extends Specification {

    def "Serialize empty params"() {
        setup:
        def req = new JsonRpcRequest("test_foo", [])
        when:
        def act = req.toJson()
        then:
        new String(act) == '{"jsonrpc":"2.0","id":1,"method":"test_foo","params":[]}'
    }

    def "Serialize single param"() {
        setup:
        def req = new JsonRpcRequest("test_foo", ["0x0000"])
        when:
        def act = req.toJson()
        then:
        new String(act) == '{"jsonrpc":"2.0","id":1,"method":"test_foo","params":["0x0000"]}'
    }

    def "Serialize two params"() {
        setup:
        def req = new JsonRpcRequest("test_foo", ["0x0000", false])
        when:
        def act = req.toJson()
        then:
        new String(act) == '{"jsonrpc":"2.0","id":1,"method":"test_foo","params":["0x0000",false]}'
    }

    def "Same requests are equal"() {
        setup:
        def req1 = new JsonRpcRequest("test_foo", ["0x0000", false])
        def req2 = new JsonRpcRequest("test_foo", ["0x0000", false])
        when:
        def act = req1.equals(req2)
        then:
        act == true
    }
}
