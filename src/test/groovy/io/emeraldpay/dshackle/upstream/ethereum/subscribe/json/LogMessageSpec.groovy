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
package io.emeraldpay.dshackle.upstream.ethereum.subscribe.json

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.Global
import io.emeraldpay.etherjar.domain.Address
import io.emeraldpay.etherjar.domain.BlockHash
import io.emeraldpay.etherjar.domain.TransactionId
import io.emeraldpay.etherjar.hex.Hex32
import io.emeraldpay.etherjar.hex.HexData
import spock.lang.Specification

class LogMessageSpec extends Specification {

    def "Serialize to a correct JSON"() {
        setup:
        def msg = new LogMessage(
                Address.from("0x011b6e24ffb0b5f5fcc564cf4183c5bbbc96d515"),
                BlockHash.from("0x48249c81bfced2e6fe2536126471b73d83c4f21de75f88a16feb57cc566b991b"),
                0xccf6e2,
                HexData.from("0x0000000000000000000000004dbd4fc535ac27206064b68ffcf827b0a60bab3f0000000000000000000000000000000000000000000000000000000000000009000000000000000000000000290328354c99b119d891a30d326bad92e36e78596782b7e23208a269f0b8262da05a2e22f4befd147ca89a47991d04b541087789"),
                0xe7,
                [
                        Hex32.from("0x23be8e12e420b5da9fb98d8102572f640fb3c11a0085060472dfc0ed194b3cf7"),
                        Hex32.from("0x000000000000000000000000000000000000000000000000000000000002bcff"),
                        Hex32.from("0xd3847bbd7bdf7bf84c0a165d198f956f7ccffebdaf1413b5a4a77980d8b6a890")
                ],
                TransactionId.from("0xc7529e79f78f58125abafeaea01fe3abdc6f45c173d5dfb36716cbc526e5b2d1"),
                0xa3,
                false,
                "LogMessageSpec")
        ObjectMapper objectMapper = Global.getObjectMapper()
        def exp = '{' +
                '"address":"0x011b6e24ffb0b5f5fcc564cf4183c5bbbc96d515",' +
                '"blockHash":"0x48249c81bfced2e6fe2536126471b73d83c4f21de75f88a16feb57cc566b991b",' +
                '"blockNumber":"0xccf6e2",' +
                '"data":"0x0000000000000000000000004dbd4fc535ac27206064b68ffcf827b0a60bab3f0000000000000000000000000000000000000000000000000000000000000009000000000000000000000000290328354c99b119d891a30d326bad92e36e78596782b7e23208a269f0b8262da05a2e22f4befd147ca89a47991d04b541087789",' +
                '"logIndex":"0xe7",' +
                '"topics":[' +
                '"0x23be8e12e420b5da9fb98d8102572f640fb3c11a0085060472dfc0ed194b3cf7",' +
                '"0x000000000000000000000000000000000000000000000000000000000002bcff",' +
                '"0xd3847bbd7bdf7bf84c0a165d198f956f7ccffebdaf1413b5a4a77980d8b6a890"' +
                '],' +
                '"transactionHash":"0xc7529e79f78f58125abafeaea01fe3abdc6f45c173d5dfb36716cbc526e5b2d1",' +
                '"transactionIndex":"0xa3",' +
                '"removed":false' +
                '}'
        when:
        def json = objectMapper.writeValueAsString(msg)

        then:
        json == exp
    }
}
