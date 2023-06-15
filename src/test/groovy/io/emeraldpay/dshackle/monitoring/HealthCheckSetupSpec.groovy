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

import io.emeraldpay.dshackle.config.HealthConfig
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.MultistreamHolder
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.api.Chain
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
        _ * ethereumUpstreams.getAll() >> [up1]
        1 * up1.status >> UpstreamAvailability.OK
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
        _ * bitcoinUpstreams.getAll() >> [up1]
        1 * up1.status >> UpstreamAvailability.OK
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
        _ * ethereumUpstreams.getAll() >> [up1, up2, up3]
        1 * up1.status >> UpstreamAvailability.OK
        1 * up2.status >> UpstreamAvailability.SYNCING
        1 * up3.status >> UpstreamAvailability.OK
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
        1 * ethereumUpstreams.getAll() >> [up1, up2, up3]
        1 * up1.status >> UpstreamAvailability.OK
        1 * up2.status >> UpstreamAvailability.SYNCING
        1 * up3.status >> UpstreamAvailability.LAGGING
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
        _ * ethereumUpstreams.getAll() >> [up1, up2, up3]
        _ * up1.status >> UpstreamAvailability.OK
        _ * up2.status >> UpstreamAvailability.SYNCING
        _ * up3.status >> UpstreamAvailability.OK
    }
}
