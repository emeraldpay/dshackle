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
package io.emeraldpay.dshackle.monitoring

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.config.HealthConfig
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.MultistreamHolder
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import spock.lang.Specification

class HealthCheckSetupSpec extends Specification {

    def "OK when meets availability - 1"() {
        setup:
        def config = new HealthConfig().tap {
            it.chains[Chain.ETHEREUM] = new HealthConfig.ChainConfig(
                    Chain.ETHEREUM, 1
            )
        }
        def up1 = Mock(Upstream)
        def ethereumUpstreams = Mock(Multistream)
        def multistream = Mock(MultistreamHolder)
        def check = new HealthCheckSetup(config, multistream)

        when:
        def act = check.health

        then:
        act.ok
        act.details == ["OK"]
        1 * multistream.getUpstream(Chain.ETHEREUM) >> ethereumUpstreams
        1 * ethereumUpstreams.available >> true
        1 * ethereumUpstreams.getAll() >> [up1]
        1 * up1.isAvailable() >> true
    }

    def "OK when meets availability - 1 - bitcoin"() {
        setup:
        def config = new HealthConfig().tap {
            it.chains[Chain.BITCOIN] = new HealthConfig.ChainConfig(
                    Chain.BITCOIN, 1
            )
        }
        def up1 = Mock(Upstream)
        def bitcoinUpstreams = Mock(Multistream)
        def multistream = Mock(MultistreamHolder)
        def check = new HealthCheckSetup(config, multistream)

        when:
        def act = check.health

        then:
        act.ok
        act.details == ["OK"]
        1 * multistream.getUpstream(Chain.BITCOIN) >> bitcoinUpstreams
        1 * bitcoinUpstreams.available >> true
        1 * bitcoinUpstreams.getAll() >> [up1]
        1 * up1.isAvailable() >> true
    }

    def "OK when meets availability - 2/3"() {
        setup:
        def config = new HealthConfig().tap {
            it.chains[Chain.ETHEREUM] = new HealthConfig.ChainConfig(
                    Chain.ETHEREUM, 2
            )
        }
        def up1 = Mock(Upstream)
        def up2 = Mock(Upstream)
        def up3 = Mock(Upstream)
        def ethereumUpstreams = Mock(Multistream)
        def multistream = Mock(MultistreamHolder)
        def check = new HealthCheckSetup(config, multistream)

        when:
        def act = check.health

        then:
        act.ok
        act.details == ["OK"]
        1 * multistream.getUpstream(Chain.ETHEREUM) >> ethereumUpstreams
        1 * ethereumUpstreams.available >> true
        1 * ethereumUpstreams.getAll() >> [up1, up2, up3]
        1 * up1.isAvailable() >> true
        1 * up2.isAvailable() >> false
        1 * up3.isAvailable() >> true
    }

    def "OK when doesn't meet availability - 2/3"() {
        setup:
        def config = new HealthConfig().tap {
            it.chains[Chain.ETHEREUM] = new HealthConfig.ChainConfig(
                    Chain.ETHEREUM, 2
            )
        }
        def up1 = Mock(Upstream)
        def up2 = Mock(Upstream)
        def up3 = Mock(Upstream)
        def ethereumUpstreams = Mock(Multistream)
        def multistream = Mock(MultistreamHolder)
        def check = new HealthCheckSetup(config, multistream)

        when:
        def act = check.health

        then:
        !act.ok
        act.details != ["OK"]
        1 * multistream.getUpstream(Chain.ETHEREUM) >> ethereumUpstreams
        1 * ethereumUpstreams.available >> true
        1 * ethereumUpstreams.getAll() >> [up1, up2, up3]
        1 * up1.isAvailable() >> true
        1 * up2.isAvailable() >> false
        1 * up3.isAvailable() >> false
    }

    def "OK when meets availability - 2/3 - detailed"() {
        setup:
        def config = new HealthConfig().tap {
            it.chains[Chain.ETHEREUM] = new HealthConfig.ChainConfig(
                    Chain.ETHEREUM, 2
            )
        }
        def up1 = Mock(Upstream)
        def up2 = Mock(Upstream)
        def up3 = Mock(Upstream)
        def ethereumUpstreams = Mock(Multistream)
        def multistream = Mock(MultistreamHolder)
        def check = new HealthCheckSetup(config, multistream)

        when:
        def act = check.detailedHealth

        then:
        act.ok
        act.details.size() > 1
        1 * multistream.getAvailable() >> [Chain.ETHEREUM]
        1 * multistream.getUpstream(Chain.ETHEREUM) >> ethereumUpstreams
        1 * ethereumUpstreams.available >> true
        1 * ethereumUpstreams.getAll() >> [up1, up2, up3]
        _ * up1.isAvailable() >> true
        _ * up2.isAvailable() >> false
        _ * up3.isAvailable() >> true
    }
}
