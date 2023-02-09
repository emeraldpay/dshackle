/**
 * Copyright (c) 2019 ETCDEV GmbH
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

import io.emeraldpay.dshackle.Chain
import spock.lang.Specification

class ChainsConfigReaderSpec extends Specification {

    ChainsConfigReader reader = new ChainsConfigReader()

    def "Parse standard config"() {
        setup:
        def stream = this.class.getClassLoader().getResourceAsStream("configs/chains-basic.yaml")
        when:
        def config = reader.read(stream)
        def act = config.resolve(Chain.ETHEREUM)
        def act2 = config.resolve(Chain.POLYGON)
        then:
        act.laggingLagSize == 5
        act.syncingLagSize == 10

        act2.laggingLagSize == 1
        act2.syncingLagSize == 6
    }
}
