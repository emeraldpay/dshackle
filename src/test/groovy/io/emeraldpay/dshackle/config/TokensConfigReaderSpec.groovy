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
package io.emeraldpay.dshackle.config

import io.emeraldpay.grpc.Chain
import spock.lang.Specification

class TokensConfigReaderSpec extends Specification {

    TokensConfigReader reader = new TokensConfigReader()

    def "Read basic"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("tokens/basic.yaml")
        when:
        def act = reader.read(config)

        then:
        act != null
        act.tokens.size() == 2
        with(act.tokens[0]) {
            id == "dai"
            blockchain == Chain.ETHEREUM
            name == "DAI"
            type == TokensConfig.Type.ERC20
            address == "0x6B175474E89094C44Da98b954EedeAC495271d0F"
        }
        with(act.tokens[1]) {
            id == "tether"
            blockchain == Chain.ETHEREUM
            name == "Tether"
            type == TokensConfig.Type.ERC20
            address == "0xdac17f958d2ee523a2206206994597c13d831ec7"
        }

    }
}
