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

import java.time.Duration

class ChainsConfigReaderSpec extends Specification {

    ChainsConfigReader reader = new ChainsConfigReader(
            new UpstreamsConfigReader(Stub(FileResolver))
    )

    def "Parse standard config"() {
        setup:
        def stream = this.class.getClassLoader().getResourceAsStream("configs/chains-basic.yaml")
        when:
        def config = reader.read(stream)
        def eth = config.resolve(Chain.ETHEREUM__MAINNET)
        def pol = config.resolve(Chain.POLYGON_POS__MAINNET)
        def opt = config.resolve(Chain.OPTIMISM__MAINNET)
        def sep = config.resolve(Chain.ETHEREUM__SEPOLIA)
        then:
        eth.laggingLagSize == 1
        eth.syncingLagSize == 6
        eth.callLimitContract == "0x32268860cAAc2948Ab5DdC7b20db5a420467Cf96"
        eth.expectedBlockTime == Duration.ofSeconds(12)

        pol.laggingLagSize == 10
        pol.syncingLagSize == 20
        pol.expectedBlockTime == Duration.ofMillis(2700)

        opt.laggingLagSize == 3
        opt.syncingLagSize == 40
        opt.options.validatePeers == false
        opt.expectedBlockTime == Duration.ofMillis(400)

        sep.laggingLagSize == 1
        sep.syncingLagSize == 10
        sep.expectedBlockTime == Duration.ofSeconds(12)
    }
}
