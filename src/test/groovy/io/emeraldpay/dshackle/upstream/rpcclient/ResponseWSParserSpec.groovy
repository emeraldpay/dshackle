/**
 * Copyright (c) 2021 EmeraldPay, Inc
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

class ResponseWSParserSpec extends Specification {

    ResponseWSParser parser = new ResponseWSParser()

    def "Parse subscription response"() {
        setup:
        def msg = "{\n" +
                "    \"id\": \"blocks\", \n" +
                "    \"jsonrpc\": \"2.0\", \n" +
                "    \"result\": \"0x9cef478923ff08bf67fde6c64013158d\"\n" +
                "}"
        when:
        def act = parser.parse(msg.bytes)
        then:
        act.type == ResponseWSParser.Type.RPC
        act.id.asString() == "blocks"
        act.error == null
        act.value == "\"0x9cef478923ff08bf67fde6c64013158d\"".bytes
    }

    def "Parse newHeads event"() {
        setup:
        def msg = "{\n" +
                "  \"jsonrpc\": \"2.0\",\n" +
                "  \"method\": \"eth_subscription\",\n" +
                "  \"params\": {\n" +
                "    \"result\": {\n" +
                "      \"difficulty\": \"0x15d9223a23aa\",\n" +
                "      \"extraData\": \"0xd983010305844765746887676f312e342e328777696e646f7773\",\n" +
                "      \"gasLimit\": \"0x47e7c4\",\n" +
                "      \"gasUsed\": \"0x38658\",\n" +
                "      \"logsBloom\": \"0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000\",\n" +
                "      \"miner\": \"0xf8b483dba2c3b7176a3da549ad41a48bb3121069\",\n" +
                "      \"nonce\": \"0x084149998194cc5f\",\n" +
                "      \"number\": \"0x1348c9\",\n" +
                "      \"parentHash\": \"0x7736fab79e05dc611604d22470dadad26f56fe494421b5b333de816ce1f25701\",\n" +
                "      \"receiptRoot\": \"0x2fab35823ad00c7bb388595cb46652fe7886e00660a01e867824d3dceb1c8d36\",\n" +
                "      \"sha3Uncles\": \"0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347\",\n" +
                "      \"stateRoot\": \"0xb3346685172db67de536d8765c43c31009d0eb3bd9c501c9be3229203f15f378\",\n" +
                "      \"timestamp\": \"0x56ffeff8\",\n" +
                "      \"transactionsRoot\": \"0x0167ffa60e3ebc0b080cdb95f7c0087dd6c0e61413140e39d94d3468d7c9689f\"\n" +
                "    },\n" +
                "    \"subscription\": \"0x9ce59a13059e417087c02d3236a0b1cc\"\n" +
                "  }\n" +
                "}"
        when:
        def act = parser.parse(msg.bytes)
        then:
        act.type == ResponseWSParser.Type.SUBSCRIPTION
        act.id.asString() == "0x9ce59a13059e417087c02d3236a0b1cc"
        act.error == null
        with(new String(act.value)) {
            it.length() > 0
            it.startsWith("{")
            it.endsWith("}")
            it.contains("\"difficulty\": \"0x15d9223a23aa\"")
        }
    }

    def "Parse RPC with error"() {
        setup:
        def msg = "{\"jsonrpc\":\"2.0\",\"id\":151,\"error\":{\"code\":-32602,\"message\":\"invalid blocknumber\"}}"
        when:
        def act = parser.parse(msg.bytes)
        then:
        act.type == ResponseWSParser.Type.RPC
        act.id.asNumber() == 151L
        act.error != null
        act.value == null
        with(act.error) {
            it.code == -32602
            it.message == "invalid blocknumber"
        }
    }

    def "Parse RPC with null result"() {
        setup:
        def msg = "{\"jsonrpc\":\"2.0\",\"id\":100,\"result\":null}"
        when:
        def act = parser.parse(msg.bytes)
        then:
        act.type == ResponseWSParser.Type.RPC
        act.id.asNumber() == 100L
        act.error == null
        new String(act.value) == "null"
    }
}
