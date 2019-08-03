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
