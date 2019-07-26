package io.emeraldpay.dshackle.config

import spock.lang.Specification

class UpstreamsConfigReaderSpec extends Specification {

    UpstreamsConfigReader reader = new UpstreamsConfigReader()

    def "Parse standard config"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("upstreams-basic.yaml")
        when:
        def act = reader.read(config)
        then:
        act != null
        act.version == "v1"
        with(act.defaultOptions) {
            size() == 1
            with(get(0)) {
                chains == ["ethereum"]
                options.quorum == 1
                options.minPeers == 3
                options.disableSyncing
            }
        }
        act.upstreams.size() == 2
        with(act.upstreams.get(0)) {
            id == "local"
            chain == "ethereum"
            provider == "geth"
            endpoints.size() == 2
            with(endpoints.get(0)) {
                type == UpstreamsConfig.EndpointType.JSON_RPC
                url == new URI("http://localhost:8545")
            }
            with(endpoints.get(1)) {
                type == UpstreamsConfig.EndpointType.WEBSOCKET
                url == new URI("ws://localhost:8546")
            }
        }
        with(act.upstreams.get(1)) {
            id == "infura"
            chain == "ethereum"
            provider == "infura"
            endpoints.size() == 1
            with(endpoints.get(0)) {
                type == UpstreamsConfig.EndpointType.JSON_RPC
                url == new URI("https://mainnet.infura.io/v3/fa28c968191849c1aff541ad1d8511f2")
                auth instanceof UpstreamsConfig.BasicAuth
                with((UpstreamsConfig.BasicAuth)auth) {
                    key == "4fc258fe41a68149c199ad8f281f2015"
                }
            }

        }
    }

    def "Parse ds config"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("upstreams-ds.yaml")
        when:
        def act = reader.read(config)
        then:
        act != null
        act.version == "v1"
        act.upstreams.size() == 1
        with(act.upstreams.get(0)) {
            id == "remote"
            chain == "auto"
            provider == "dshackle"
            endpoints.size() == 1
            with(endpoints.get(0)) {
                type == UpstreamsConfig.EndpointType.DSHACKLE
                host == "10.2.0.15"
                auth instanceof UpstreamsConfig.TlsAuth
                with((UpstreamsConfig.TlsAuth)auth) {
                    ca == "/etc/ca.myservice.com.crt"
                    certificate == "/etc/client1.myservice.com.crt"
                    key == "/etc/client1.myservice.com.key"
                }
            }
        }
    }
}
