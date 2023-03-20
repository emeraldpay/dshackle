package io.emeraldpay.dshackle.upstream.bitcoin

import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.test.StandardApiReaderMock
import io.emeraldpay.etherjar.rpc.RpcResponseError
import spock.lang.Specification

import java.time.Duration

import static io.emeraldpay.dshackle.upstream.UpstreamAvailability.IMMATURE
import static io.emeraldpay.dshackle.upstream.UpstreamAvailability.OK
import static io.emeraldpay.dshackle.upstream.UpstreamAvailability.UNAVAILABLE

class BitcoinUpstreamValidatorSpec extends Specification {

    def "Doesnt check getconnectioncount when all disabled"() {
        setup:
        def options = UpstreamsConfig.PartialOptions.getDefaults().tap {
            it.disableValidation = true
        }.build()
        def up = Mock(io.emeraldpay.dshackle.reader.Reader)
        def validator = new BitcoinUpstreamValidator(up, options)

        when:
        def act = validator.validate().block(Duration.ofSeconds(1))
        then:
        act == OK
        0 * up.read()
    }

    def "Doesnt check getconnectioncount when minPeers is zero"() {
        setup:
        def options = UpstreamsConfig.PartialOptions.getDefaults().tap {
            it.disableValidation = false
            it.validatePeers = true
            it.minPeers = 0
        }.build()
        def up = Mock(io.emeraldpay.dshackle.reader.Reader)
        def validator = new BitcoinUpstreamValidator(up, options)

        when:
        def act = validator.validate().block(Duration.ofSeconds(1))
        then:
        act == OK
        0 * up.read()
    }

    def "Doesnt check getconnectioncount when peers validations is disabled"() {
        setup:
        def options = UpstreamsConfig.PartialOptions.getDefaults().tap {
            it.disableValidation = false
            it.validatePeers = false
            it.minPeers = 10
        }.build()
        def up = Mock(io.emeraldpay.dshackle.reader.Reader)
        def validator = new BitcoinUpstreamValidator(up, options)

        when:
        def act = validator.validate().block(Duration.ofSeconds(1))
        then:
        act == OK
        0 * up.read()
    }

    def "Peers is IMMATURE when state returned too few peers"() {
        setup:
        def options = UpstreamsConfig.PartialOptions.getDefaults().tap {
            it.disableValidation = false
            it.validatePeers = true
            it.minPeers = 10
        }.build()
        def up = new StandardApiReaderMock()
                .answerOnce("getconnectioncount", [], 5)
        def validator = new BitcoinUpstreamValidator(up, options)

        when:
        def act = validator.validate().block(Duration.ofSeconds(1))
        then:
        act == IMMATURE
    }

    def "Peers is OK when state returned exact peers"() {
        setup:
        def options = UpstreamsConfig.PartialOptions.getDefaults().tap {
            it.disableValidation = false
            it.validatePeers = true
            it.minPeers = 10
        }.build()
        def up = new StandardApiReaderMock()
                .answerOnce("getconnectioncount", [], 10)
        def validator = new BitcoinUpstreamValidator(up, options)

        when:
        def act = validator.validate().block(Duration.ofSeconds(1))
        then:
        act == OK
    }

    def "Peers is OK when state returned more peers"() {
        setup:
        def options = UpstreamsConfig.PartialOptions.getDefaults().tap {
            it.disableValidation = false
            it.validatePeers = true
            it.minPeers = 10
        }.build()
        def up = new StandardApiReaderMock()
                .answerOnce("getconnectioncount", [], 20)
        def validator = new BitcoinUpstreamValidator(up, options)

        when:
        def act = validator.validate().block(Duration.ofSeconds(1))
        then:
        act == OK
    }

    def "Peers is UNAVAILABLE when state returned error"() {
        setup:
        def options = UpstreamsConfig.PartialOptions.getDefaults().tap {
            it.disableValidation = false
            it.validatePeers = true
            it.minPeers = 10
        }.build()
        def up = new StandardApiReaderMock()
                .answerOnce("getconnectioncount", [], new RpcResponseError(RpcResponseError.CODE_METHOD_NOT_EXIST, "Unavailable"))
        def validator = new BitcoinUpstreamValidator(up, options)

        when:
        def act = validator.validate().block(Duration.ofSeconds(1))
        then:
        act == UNAVAILABLE
    }
}
