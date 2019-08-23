/**
 * Copyright (c) 2019 ETCDEV GmbH
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
package io.emeraldpay.dshackle.upstream

import spock.lang.Specification

class UpstreamAvailabilitySpec extends Specification {

    def "Defined order"() {
        expect:
        UpstreamAvailability.OK.compareTo(UpstreamAvailability.LAGGING) < 0
        UpstreamAvailability.LAGGING.compareTo(UpstreamAvailability.IMMATURE) < 0
        UpstreamAvailability.IMMATURE.compareTo(UpstreamAvailability.SYNCING) < 0
        UpstreamAvailability.SYNCING.compareTo(UpstreamAvailability.UNAVAILABLE) < 0

        UpstreamAvailability.OK.compareTo(UpstreamAvailability.UNAVAILABLE) < 0
        UpstreamAvailability.UNAVAILABLE.compareTo(UpstreamAvailability.OK) > 0
    }

    def "Sort array"() {
        setup:
        def items = [UpstreamAvailability.OK, UpstreamAvailability.SYNCING, UpstreamAvailability.IMMATURE, UpstreamAvailability.OK]
        when:
        Collections.sort(items)
        then:
        items == [UpstreamAvailability.OK, UpstreamAvailability.OK, UpstreamAvailability.IMMATURE, UpstreamAvailability.SYNCING]
    }

    def "First is most available"() {
        expect:
        [UpstreamAvailability.UNAVAILABLE, UpstreamAvailability.OK].toSorted().first() == UpstreamAvailability.OK
        [UpstreamAvailability.OK, UpstreamAvailability.UNAVAILABLE].toSorted().first() == UpstreamAvailability.OK
        [UpstreamAvailability.OK, UpstreamAvailability.LAGGING].toSorted().first() == UpstreamAvailability.OK
        [UpstreamAvailability.LAGGING, UpstreamAvailability.UNAVAILABLE].toSorted().first() == UpstreamAvailability.LAGGING
        [UpstreamAvailability.OK, UpstreamAvailability.SYNCING].toSorted().first() == UpstreamAvailability.OK
        [UpstreamAvailability.SYNCING, UpstreamAvailability.UNAVAILABLE].toSorted().first() == UpstreamAvailability.SYNCING
    }
}
