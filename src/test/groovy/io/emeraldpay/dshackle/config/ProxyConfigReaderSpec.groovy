package io.emeraldpay.dshackle.config

import io.emeraldpay.grpc.Chain
import spock.lang.Specification

class ProxyConfigReaderSpec extends Specification {

    ProxyConfigReader reader = new ProxyConfigReader()

    def "Read basic proxy config"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("dshackle-proxy-basic.yaml")
        when:
        def act = reader.read(config)

        then:
        act.enabled
        act.port == 8080
        act.host == '127.0.0.1'
        act.routes.size() == 1
        with(act.routes[0]) {
            id == "ethereum"
            blockchain == Chain.ETHEREUM
        }
    }

    def "Read proxy config with two elements"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("dshackle-proxy-two.yaml")
        when:
        def act = reader.read(config)

        then:
        act.enabled
        act.port == 8080
        act.routes.size() == 2
        with(act.routes[0]) {
            id == "ethereum"
            blockchain == Chain.ETHEREUM
        }
        with(act.routes[1]) {
            id == "classic"
            blockchain == Chain.ETHEREUM_CLASSIC
        }
    }

    def "Read max proxy config"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("dshackle-proxy-max.yaml")
        when:
        def act = reader.read(config)

        then:
        act.enabled
        act.host == '0.0.0.0'
        act.port == 8080
        act.routes.size() == 2
        with(act.routes[0]) {
            id == "ethereum"
            blockchain == Chain.ETHEREUM
        }
        with(act.routes[1]) {
            id == "classic"
            blockchain == Chain.ETHEREUM_CLASSIC
        }
    }
}
