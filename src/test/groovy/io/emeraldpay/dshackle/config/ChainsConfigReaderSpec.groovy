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
import io.emeraldpay.dshackle.FileResolver
import spock.lang.Specification

class ChainsConfigReaderSpec extends Specification {

    ChainsConfigReader reader = new ChainsConfigReader(
            new UpstreamsConfigReader(Stub(FileResolver))
    )

    def "Parse standard config"() {
        setup:
        def stream = this.class.getClassLoader().getResourceAsStream("configs/chains-basic.yaml")
        when:
        def config = reader.read(stream)
        def eth = config.resolve(Chain.ETHEREUM)
        def pol = config.resolve(Chain.POLYGON)
        def opt = config.resolve(Chain.OPTIMISM)
        def sep = config.resolve(Chain.TESTNET_SEPOLIA)
        then:
        eth.laggingLagSize == 1
        eth.syncingLagSize == 6

        pol.laggingLagSize == 10
        pol.syncingLagSize == 20

        opt.laggingLagSize == 3
        opt.syncingLagSize == 40
        opt.options.validatePeers == false

        sep.laggingLagSize == 1
        sep.syncingLagSize == 10

        eth.laggingLagSize == 1
        eth.syncingLagSize == 6
    }
}
