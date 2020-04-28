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

class ProxyConfigReaderSpec extends Specification {

    ProxyConfigReader reader = new ProxyConfigReader()

    def "Read basic proxy config"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("dshackle-proxy-basic.yaml")
        when:
        def act = reader.read(config)

        then:
        act.enabled
        act.port == 8080
        act.host == '127.0.0.1'
        act.routes.size() == 1
        with(act.routes[0]) {
            id == "ethereum"
            blockchain == Chain.ETHEREUM
        }
    }

    def "Read proxy config with two elements"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("dshackle-proxy-two.yaml")
        when:
        def act = reader.read(config)

        then:
        act.enabled
        act.port == 8080
        act.routes.size() == 2
        with(act.routes[0]) {
            id == "ethereum"
            blockchain == Chain.ETHEREUM
        }
        with(act.routes[1]) {
            id == "classic"
            blockchain == Chain.ETHEREUM_CLASSIC
        }
    }

    def "Read max proxy config"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("dshackle-proxy-max.yaml")
        when:
        def act = reader.read(config)

        then:
        act.enabled
        act.host == '0.0.0.0'
        act.port == 8080
        act.routes.size() == 2
        with(act.routes[0]) {
            id == "ethereum"
            blockchain == Chain.ETHEREUM
        }
        with(act.routes[1]) {
            id == "classic"
            blockchain == Chain.ETHEREUM_CLASSIC
        }
    }
}
