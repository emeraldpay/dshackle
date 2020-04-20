package io.emeraldpay.dshackle.rpc

import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.upstream.Upstreams
import io.emeraldpay.grpc.Chain
import spock.lang.Specification

class TrackBitcoinAddressSpec extends Specification {

    def "Correct sum from multiple"() {
        setup:
        def json = this.class.getClassLoader().getResourceAsStream("bitcoin/unspent-one-addr.json")
        def unspents = TestingCommons.objectMapper().readValue(json, List)
        TrackBitcoinAddress track = new TrackBitcoinAddress(Stub(Upstreams))
        when:
        def total = track.getTotal(Chain.BITCOIN, ["1K7xkspJg7DDKNwzXgoRSDCUxiFsRegsSK"], unspents)

        then:
        total.size() == 1
        total[0].chain == Chain.BITCOIN
        total[0].address == "1K7xkspJg7DDKNwzXgoRSDCUxiFsRegsSK"
        total[0].balance.toString() == "32928461"
    }

    def "Correct sum when other addresses"() {
        setup:
        def json = this.class.getClassLoader().getResourceAsStream("bitcoin/unspent-two-addr.json")
        def unspents = TestingCommons.objectMapper().readValue(json, List)
        TrackBitcoinAddress track = new TrackBitcoinAddress(Stub(Upstreams))
        when:
        def total = track.getTotal(Chain.BITCOIN, ["1K7xkspJg7DDKNwzXgoRSDCUxiFsRegsSK"], unspents)

        then:
        total.size() == 1
        total[0].chain == Chain.BITCOIN
        total[0].address == "1K7xkspJg7DDKNwzXgoRSDCUxiFsRegsSK"
        total[0].balance.toString() == "32928461"
    }

    def "Sum for two addresses"() {
        setup:
        def json = this.class.getClassLoader().getResourceAsStream("bitcoin/unspent-two-addr.json")
        def unspents = TestingCommons.objectMapper().readValue(json, List)
        TrackBitcoinAddress track = new TrackBitcoinAddress(Stub(Upstreams))
        when:
        def total = track.getTotal(Chain.BITCOIN, ["1K7xkspJg7DDKNwzXgoRSDCUxiFsRegsSK", "35hK24tcLEWcgNA4JxpvbkNkoAcDGqQPsP"], unspents).sort { it.address }

        then:
        total.size() == 2
        with(total[0]) {
            chain == Chain.BITCOIN
            address == "1K7xkspJg7DDKNwzXgoRSDCUxiFsRegsSK"
            balance.toString() == "32928461"
        }
        with(total[1]) {
            chain == Chain.BITCOIN
            address == "35hK24tcLEWcgNA4JxpvbkNkoAcDGqQPsP"
            balance.toString() == "25550215615737"
        }
    }

    def "Zero for empty unspents"() {
        setup:
        TrackBitcoinAddress track = new TrackBitcoinAddress(Stub(Upstreams))
        when:
        def total = track.getTotal(Chain.BITCOIN, ["1K7xkspJg7DDKNwzXgoRSDCUxiFsRegsSK"], [])

        then:
        total.size() == 1
        total[0].chain == Chain.BITCOIN
        total[0].address == "1K7xkspJg7DDKNwzXgoRSDCUxiFsRegsSK"
        total[0].balance.toString() == "0"
    }

    def "Zero for unknown address"() {
        setup:
        def json = this.class.getClassLoader().getResourceAsStream("bitcoin/unspent-two-addr.json")
        def unspents = TestingCommons.objectMapper().readValue(json, List)
        TrackBitcoinAddress track = new TrackBitcoinAddress(Stub(Upstreams))
        when:
        def total = track.getTotal(Chain.BITCOIN, ["16rCmCmbuWDhPjWTrpQGaU3EPdZF7MTdUk", "1K7xkspJg7DDKNwzXgoRSDCUxiFsRegsSK"], unspents).sort { it.address }

        then:
        total.size() == 2
        with(total[0]) {
            address == "16rCmCmbuWDhPjWTrpQGaU3EPdZF7MTdUk"
            balance.toString() == "0"
        }
        with(total[1]) {
            address == "1K7xkspJg7DDKNwzXgoRSDCUxiFsRegsSK"
            balance.toString() == "32928461"
        }
    }

}
