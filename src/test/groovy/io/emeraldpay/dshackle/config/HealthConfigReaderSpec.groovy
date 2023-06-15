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
package io.emeraldpay.dshackle.config

import io.emeraldpay.api.Chain
import spock.lang.Specification

class HealthConfigReaderSpec extends Specification {

    HealthConfigReader reader = new HealthConfigReader()

    def "Read empty"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("configs/dshackle-health-empty.yaml")
        when:
        def act = reader.read(config)

        then:
        !act.enabled
        act.port == 8082
        act.host == "127.0.0.1"
        act.path == "/health"
        act.configs().size() == 0
    }

    def "Read single"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("configs/dshackle-health-1.yaml")
        when:
        def act = reader.read(config)

        then:
        act.enabled
        act.port == 8082
        act.host == "127.0.0.1"
        act.path == "/health"
        with(act.configs()) {
            size() == 1
            with(it[0]) {
                it.blockchain == Chain.ETHEREUM
                it.minAvailable == 2
            }
        }
    }

    def "Read multiple"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("configs/dshackle-health-2.yaml")
        when:
        def act = reader.read(config)

        then:
        act.enabled
        act.port == 10003
        act.host == "0.0.0.0"
        act.path == "/healtz"
        with(act.configs().toSorted { it.blockchain.id }) {
            size() == 2
            with(it[0]) {
                it.blockchain == Chain.BITCOIN
                it.minAvailable == 1
            }
            with(it[1]) {
                it.blockchain == Chain.ETHEREUM
                it.minAvailable == 2
            }
        }
    }
}
