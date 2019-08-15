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
                options.minPeers == 3
            }
        }
        act.upstreams.size() == 2
        with(act.upstreams.get(0)) {
            id == "local"
            chain == "ethereum"
            connection instanceof UpstreamsConfig.EthereumConnection
            with((UpstreamsConfig.EthereumConnection)connection) {
                rpc != null
                rpc.url == new URI("http://localhost:8545")
                ws != null
                ws.url == new URI("ws://localhost:8546")
            }
        }
        with(act.upstreams.get(1)) {
            id == "infura"
            chain == "ethereum"
            connection instanceof UpstreamsConfig.EthereumConnection
            with((UpstreamsConfig.EthereumConnection)connection) {
                rpc.url == new URI("https://mainnet.infura.io/v3/fa28c968191849c1aff541ad1d8511f2")
                rpc.basicAuth != null
                with((UpstreamsConfig.BasicAuth)rpc.basicAuth) {
                    username == "4fc258fe41a68149c199ad8f281f2015"
                    password == "1a68f20154fc258fe4149c199ad8f281"
                }
                ws == null
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
            connection instanceof UpstreamsConfig.GrpcConnection
            with((UpstreamsConfig.GrpcConnection)connection) {
                host == "10.2.0.15"
                auth != null
                with(auth) {
                    ca == "/etc/ca.myservice.com.crt"
                    certificate == "/etc/client1.myservice.com.crt"
                    key == "/etc/client1.myservice.com.key"
                }
            }
        }
    }

    def "Parse config with labels"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("upstreams-labels.yaml")
        when:
        def act = reader.read(config)
        then:
        act != null
        act.upstreams.size() == 2
        with(act.upstreams.get(0)) {
            connection instanceof UpstreamsConfig.GrpcConnection
            labels.isEmpty()
        }
        with(act.upstreams.get(1)) {
            !labels.isEmpty()
            labels.size() == 2
            labels["fullnode"] == "true"
            labels["api"] == "geth"
        }
    }

    def "Parse config with options"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("upstreams-options.yaml")
        when:
        def act = reader.read(config)
        then:
        act != null
        act.upstreams.size() == 2
        with(act.upstreams.get(0)) {
            options.minPeers == 7
        }
        with(act.upstreams.get(1)) {
            options.disableValidation == true
        }
    }

    def "Post process for usual strings"() {
        expect:
        s == reader.postProcess(s)
        where:
        s << ["", "a", "13143", "/etc/client1.myservice.com.key", "true", "1a68f20154fc258fe4149c199ad8f281"]
    }

    def "Post process replaces from env"() {
        setup:
        System.setProperty("id", "1")
        System.setProperty("HOME", "/home/user")
        System.setProperty("PASSWORD", "1a68f20154fc258fe4149c199ad8f281")
        expect:
        replaced == reader.postProcess(orig)
        where:
        orig                | replaced
        "p_\${id}"          | "p_1"
        "home: \${HOME}"    | "home: /home/user"
        "\${PASSWORD}"      | "1a68f20154fc258fe4149c199ad8f281"
    }

}
