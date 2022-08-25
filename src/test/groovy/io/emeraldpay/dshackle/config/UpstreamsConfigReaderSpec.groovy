/**
 * Copyright (c) 2019 ETCDEV GmbH
 * Copyright (c) 2020 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.config

import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.upstream.ethereum.EthereumUpstream
import io.emeraldpay.grpc.Chain
import io.emeraldpay.etherjar.rpc.RpcClient
import spock.lang.Specification

class UpstreamsConfigReaderSpec extends Specification {

    UpstreamsConfigReader reader = new UpstreamsConfigReader(TestingCommons.fileResolver())

    def "Parse standard config"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("upstreams-basic.yaml")
        when:
        def act = reader.read(config)
        then:
        act != null
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
                ws.basicAuth != null
                with(ws.basicAuth) {
                    username == "9c199ad8f281f20154fc258fe41a6814"
                    password == "258fe4149c199ad8f2811a68f20154fc"
                }
            }
        }
        with(act.upstreams.get(1)) {
            id == "infura"
            chain == "ethereum"
            connection instanceof UpstreamsConfig.EthereumConnection
            with((UpstreamsConfig.EthereumConnection)connection) {
                rpc.url == new URI("https://mainnet.infura.io/v3/fa28c968191849c1aff541ad1d8511f2")
                rpc.basicAuth != null
                with((AuthConfig.ClientBasicAuth) rpc.basicAuth) {
                    username == "4fc258fe41a68149c199ad8f281f2015"
                    password == "1a68f20154fc258fe4149c199ad8f281"
                }
                ws == null
            }

        }
    }

    def "Parse websocket-only config"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("upstreams-ws-only.yaml")
        when:
        def act = reader.read(config)
        then:
        act != null
        act.upstreams.size() == 1
        with(act.upstreams.get(0)) {
            id == "local"
            chain == "ethereum"
            connection instanceof UpstreamsConfig.EthereumConnection
            with((UpstreamsConfig.EthereumConnection) connection) {
                rpc == null
                ws != null
                ws.url == new URI("ws://localhost:8546")
                ws.basicAuth != null
                with(ws.basicAuth) {
                    username == "9c199ad8f281f20154fc258fe41a6814"
                    password == "258fe4149c199ad8f2811a68f20154fc"
                }
            }
        }
    }

    def "Parse priorities"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("upstreams-priority.yaml")
        when:
        def act = reader.read(config)
        then:
        act != null
        act.upstreams.size() == 3
        act.upstreams[0].options.priority == 100
        act.upstreams[1].options.priority == 50
        act.upstreams[2].options.priority == 75
    }

    def "Parse full defined websocket config"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("upstreams-ws-full.yaml")
        when:
        def act = reader.read(config)
        then:
        act != null
        act.upstreams.size() == 1
        with(act.upstreams.get(0)) {
            id == "local"
            chain == "ethereum"
            connection instanceof UpstreamsConfig.EthereumConnection
            with((UpstreamsConfig.EthereumConnection) connection) {
                rpc == null
                ws != null
                ws.url == new URI("ws://localhost:8546")
                ws.basicAuth != null
                with(ws.basicAuth) {
                    username == "9c199ad8f281f20154fc258fe41a6814"
                    password == "258fe4149c199ad8f2811a68f20154fc"
                }
                ws.frameSize == 10 * 1024 * 1024
                ws.msgSize == 25 * 1024 * 1024
            }
        }
    }

    def "Parse bitcoin upstreams"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("upstreams-bitcoin.yaml")
        when:
        def act = reader.read(config)
        then:
        act != null
        with(act.defaultOptions) {
            size() == 1
            with(get(0)) {
                chains == ["bitcoin"]
                options.minPeers == 3
            }
        }
        act.upstreams.size() == 1
        with(act.upstreams.get(0)) {
            id == "local"
            chain == "bitcoin"
            options == null || options.providesBalance == false
            connection instanceof UpstreamsConfig.BitcoinConnection
            with((UpstreamsConfig.BitcoinConnection) connection) {
                rpc != null
                rpc.url == new URI("http://localhost:8545")
                esplora == null
            }
        }
    }

    def "Parse bitcoin upstreams with esplora"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("upstreams-bitcoin-esplora.yaml")
        when:
        def act = reader.read(config)
        then:
        act != null
        with(act.defaultOptions) {
            size() == 1
            with(get(0)) {
                chains == ["bitcoin"]
                options.minPeers == 3
            }
        }
        act.upstreams.size() == 1
        with(act.upstreams.get(0)) {
            id == "local"
            chain == "bitcoin"
            options.providesBalance == true
            connection instanceof UpstreamsConfig.BitcoinConnection
            with((UpstreamsConfig.BitcoinConnection) connection) {
                rpc != null
                rpc.url == new URI("http://localhost:8545")
                esplora != null
                esplora.url == new URI("http://localhost:3001")
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

    def "Parse config without defaults"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("upstreams-no-defaults.yaml")
        when:
        def act = reader.read(config)
        then:
        act != null
        with(act.defaultOptions) {
            size() == 0
        }
        act.upstreams.size() == 1
        with(act.upstreams.get(0)) {
            id == "local"
            chain == "ethereum"
            connection instanceof UpstreamsConfig.EthereumConnection
            with((UpstreamsConfig.EthereumConnection)connection) {
                rpc != null
                rpc.url == new URI("http://localhost:8545")
                ws == null
            }
        }
    }

    def "Parse config with methods"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("upstreams-methods.yaml")
        when:
        def act = reader.read(config)
        then:
        act != null
        with(act.upstreams.get(0)) {
            methods != null
            with(methods) {
                enabled.size() == 1
                enabled.first().name == "parity_trace"

                disabled.size() == 2
                disabled.toList()[0].name == "eth_getBlockByNumber"
                disabled.toList()[1].name == "admin_shutdown"
            }
        }
    }

    def "Parse config with methods and quorum"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("upstreams-methods-quorum.yaml")
        when:
        def act = reader.read(config)
        then:
        act != null
        with(act.upstreams.get(0)) {
            methods != null
            with(methods) {
                enabled.size() == 2
                with(enabled[0]) {
                    it.name == "custom_foo"
                    it.quorum == "not_lagging"
                }
                with(enabled[1]) {
                    it.name == "custom_bar"
                    it.quorum == "not_empty"
                }
            }
        }
    }

    def "Parse config with invalid ids"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("upstreams-no-id.yaml")
        when:
        def act = reader.read(config)
        then:
        act != null
        act.upstreams.size() == 1
        with(act.upstreams.get(0)) {
            id == "test"
        }
    }

    def "Invalidate wrong ids"() {
        expect:
        !reader.isValid(new UpstreamsConfig.Upstream<UpstreamsConfig.EthereumConnection>(id: id))
        where:
        id << ["", null, "a", "ab", "!ab", "foo bar", "foo@bar", "123test", "_test", "test/test"]
    }

    def "Accept good ids"() {
        expect:
        reader.isValid(new UpstreamsConfig.Upstream<UpstreamsConfig.EthereumConnection>(id: id))
        where:
        id << ["test", "test_test", "test-test", "test123", "test1test", "foo_bar_12"]
    }

    def "Parse config without fallback role"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("upstreams-basic.yaml")
        when:
        def act = reader.read(config)
        then:
        act != null
        act.upstreams.size() == 2
        act.upstreams.get(0).role == UpstreamsConfig.UpstreamRole.PRIMARY
        act.upstreams.get(1).role == UpstreamsConfig.UpstreamRole.PRIMARY
    }

    def "Parse config with fallback role"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("upstreams-roles.yaml")
        when:
        def act = reader.read(config)
        then:
        act != null
        act.upstreams.size() == 2
        act.upstreams.get(0).role == UpstreamsConfig.UpstreamRole.PRIMARY
        act.upstreams.get(1).role == UpstreamsConfig.UpstreamRole.FALLBACK
    }

    def "Parse config with secondary role"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("upstreams-roles-2.yaml")
        when:
        def act = reader.read(config)
        then:
        act != null
        act.upstreams.size() == 3
        act.upstreams.get(0).role == UpstreamsConfig.UpstreamRole.PRIMARY
        act.upstreams.get(1).role == UpstreamsConfig.UpstreamRole.SECONDARY
        act.upstreams.get(2).role == UpstreamsConfig.UpstreamRole.FALLBACK
    }

    def "Parse config with invalid role"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("upstreams-roles-invalid.yaml")
        when:
        def act = reader.read(config)
        then:
        act != null
        act.upstreams.size() == 2
        act.upstreams.get(0).role == UpstreamsConfig.UpstreamRole.PRIMARY
        act.upstreams.get(1).role == UpstreamsConfig.UpstreamRole.PRIMARY
    }
}
