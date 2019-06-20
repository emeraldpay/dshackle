package io.emeraldpay.dshackle.config

import spock.lang.Specification

class UpstreamsReaderSpec extends Specification {

    UpstreamsReader reader = new UpstreamsReader()

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
                type == Upstreams.EndpointType.JSON_RPC
                url == new URI("http://localhost:8545")
            }
            with(endpoints.get(1)) {
                type == Upstreams.EndpointType.WEBSOCKET
                url == new URI("ws://localhost:8546")
            }
        }
        with(act.upstreams.get(1)) {
            id == "infura"
            chain == "ethereum"
            provider == "infura"
            endpoints.size() == 1
            with(endpoints.get(0)) {
                type == Upstreams.EndpointType.JSON_RPC
                url == new URI("https://mainnet.infura.io/v3/fa28c968191849c1aff541ad1d8511f2")
                auth instanceof Upstreams.BasicAuth
                with((Upstreams.BasicAuth)auth) {
                    key == "4fc258fe41a68149c199ad8f281f2015"
                }
            }

        }
    }
}
