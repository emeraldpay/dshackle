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
import spock.lang.Specification

import java.time.Duration

class UpstreamsConfigReaderSpec extends Specification {

    UpstreamsConfigReader reader = new UpstreamsConfigReader(TestingCommons.fileResolver())

    def "Parse standard config"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("configs/upstreams-basic.yaml")
        when:
        def act = reader.readInternal(config)
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
        def config = this.class.getClassLoader().getResourceAsStream("configs/upstreams-ws-only.yaml")
        when:
        def act = reader.readInternal(config)
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

    def "Parse full defined websocket config"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("configs/upstreams-ws-full.yaml")
        when:
        def act = reader.readInternal(config)
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
        def config = this.class.getClassLoader().getResourceAsStream("configs/upstreams-bitcoin.yaml")
        when:
        def act = reader.readInternal(config)
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

    def "Parse ethereum pos upstreams"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("configs/upstreams-ethereum-pos.yaml")
        when:
        def act = reader.readInternal(config)
        then:
        act != null
        act.upstreams.size() == 1
        with(act.upstreams.get(0)) {
            id == "eth2-1"
            chain == "ropsten"
            connection instanceof UpstreamsConfig.EthereumPosConnection
            with((UpstreamsConfig.EthereumPosConnection) connection) {
                execution.rpc != null
                execution.rpc.url == new URI("http://34.106.60.110:8545")
                upstreamRating == 100
            }
        }
    }

    def "Parse bitcoin upstreams with esplora"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("configs/upstreams-bitcoin-esplora.yaml")
        when:
        def act = reader.readInternal(config)
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
        def config = this.class.getClassLoader().getResourceAsStream("configs/upstreams-ds.yaml")
        when:
        def act = reader.readInternal(config)
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
        def config = this.class.getClassLoader().getResourceAsStream("configs/upstreams-labels.yaml")
        when:
        def act = reader.readInternal(config)
        then:
        act != null
        act.upstreams.size() == 2
        with(act.upstreams.get(0)) {
            connection instanceof UpstreamsConfig.GrpcConnection
            labels.size() == 1
            labels["provider"] == "some_service"
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
        def config = this.class.getClassLoader().getResourceAsStream("configs/upstreams-options.yaml")
        when:
        def act = reader.readInternal(config)
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
        def config = this.class.getClassLoader().getResourceAsStream("configs/upstreams-no-defaults.yaml")
        when:
        def act = reader.readInternal(config)
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
        def config = this.class.getClassLoader().getResourceAsStream("configs/upstreams-methods.yaml")
        when:
        def act = reader.readInternal(config)
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
        def config = this.class.getClassLoader().getResourceAsStream("configs/upstreams-methods-quorum.yaml")
        when:
        def act = reader.readInternal(config)
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
        def config = this.class.getClassLoader().getResourceAsStream("configs/upstreams-no-id.yaml")
        when:
        def act = reader.readInternal(config)
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
        def config = this.class.getClassLoader().getResourceAsStream("configs/upstreams-basic.yaml")
        when:
        def act = reader.readInternal(config)
        then:
        act != null
        act.upstreams.size() == 2
        act.upstreams.get(0).role == UpstreamsConfig.UpstreamRole.PRIMARY
        act.upstreams.get(1).role == UpstreamsConfig.UpstreamRole.PRIMARY
    }

    def "Parse config with fallback role"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("configs/upstreams-roles.yaml")
        when:
        def act = reader.readInternal(config)
        then:
        act != null
        act.upstreams.size() == 2
        act.upstreams.get(0).role == UpstreamsConfig.UpstreamRole.PRIMARY
        act.upstreams.get(1).role == UpstreamsConfig.UpstreamRole.FALLBACK
    }

    def "Parse config with secondary role"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("configs/upstreams-roles-2.yaml")
        when:
        def act = reader.readInternal(config)
        then:
        act != null
        act.upstreams.size() == 3
        act.upstreams.get(0).role == UpstreamsConfig.UpstreamRole.PRIMARY
        act.upstreams.get(1).role == UpstreamsConfig.UpstreamRole.SECONDARY
        act.upstreams.get(2).role == UpstreamsConfig.UpstreamRole.FALLBACK
    }

    def "Parse config with invalid role"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("configs/upstreams-roles-invalid.yaml")
        when:
        def act = reader.readInternal(config)
        then:
        act != null
        act.upstreams.size() == 2
        act.upstreams.get(0).role == UpstreamsConfig.UpstreamRole.PRIMARY
        act.upstreams.get(1).role == UpstreamsConfig.UpstreamRole.PRIMARY
    }

    def "Parse node id"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("configs/upstreams-node-id.yaml")
        when:
        def act = reader.readInternal(config)
        then:
        act != null
        act.upstreams.size() == 2
        with(act.upstreams.get(0)) {
            nodeId == 1
            id == "has_node_id"
        }
        with(act.upstreams.get(1)) {
            nodeId == null
            id == "has_no_node_id"
        }
    }

    def "Parse method groups"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("configs/upstreams-method-groups.yaml")
        when:
        def act = reader.readInternal(config)
        then:
        act != null
        act.upstreams.size() == 1
        with(act.upstreams.get(0)) {
            id == "enable_filter_methods"
            methodGroups.enabled.first() == "filter"
            methodGroups.disabled.first() == "trace"
        }
    }

    def "Parse config with validation options"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("configs/upstreams-validation.yaml")
        when:
        def act = reader.readInternal(config)
        then:
        act != null
        act.upstreams.size() == 3
        with(act.upstreams.get(0).options) {
            disableValidation == false
            validateSyncing == true
            validatePeers == false
        }
        with(act.upstreams.get(1).options) {
            disableValidation == false
            validateSyncing == false
            validatePeers == false
        }
        with(act.upstreams.get(2).options) {
            disableValidation == true
            validateSyncing == true
            validatePeers == true
        }
    }

    def "Parse connector mode in connection config"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("configs/upstreams-connector-mode.yaml")
        when:
        def act = reader.readInternal(config)
        then:
        act != null
        act.upstreams.size() == 1
        with(act.upstreams.get(0)) {
            id == "local"
            chain == "ethereum"
            connection instanceof UpstreamsConfig.EthereumConnection
            with((UpstreamsConfig.EthereumConnection) connection) {
                rpc != null
                rpc.url == new URI("https://localhost:8546")
                ws != null
                ws.url == new URI("ws://localhost:8546")
                connectorMode == "RPC_REQUESTS_WITH_WS_HEAD"
            }
        }
    }

    def "Merge options for disableValidation"() {
        expect:
        def a = new UpstreamsConfig.PartialOptions().tap { disableValidation = base }
        def b = new UpstreamsConfig.PartialOptions().tap { disableValidation = overwrite }
        def result = a.merge(b).buildOptions()
        result.disableValidation == exp

        where:
        base        | overwrite     | exp
        true        | true          | true
        true        | false         | false
        true        | null          | true

        false       | true          | true
        false       | false         | false
        false       | null          | false

        null        | true          | true
        null        | false         | false
        null        | null          | false
    }

    def "Merge options for providesBalance"() {
        expect:
        def a = new UpstreamsConfig.PartialOptions().tap { providesBalance = base }
        def b = new UpstreamsConfig.PartialOptions().tap { providesBalance = overwrite }
        def result = a.merge(b).buildOptions()
        result.providesBalance == exp

        where:
        base        | overwrite     | exp
        true        | true          | true
        true        | false         | false
        true        | null          | true

        false       | true          | true
        false       | false         | false
        false       | null          | false

        null        | true          | true
        null        | false         | false
        null        | null          | null
    }

    def "Merge options for validatePeers"() {
        expect:
        def a = new UpstreamsConfig.PartialOptions().tap { validatePeers = base }
        def b = new UpstreamsConfig.PartialOptions().tap { validatePeers = overwrite }
        def result = a.merge(b).buildOptions()
        result.validatePeers == exp

        where:
        base        | overwrite     | exp
        true        | true          | true
        true        | false         | false
        true        | null          | true

        false       | true          | true
        false       | false         | false
        false       | null          | false

        null        | true          | true
        null        | false         | false
        null        | null          | true
    }

    def "Merge options for validateSyncing"() {
        expect:
        def a = new UpstreamsConfig.PartialOptions().tap { validateSyncing = base }
        def b = new UpstreamsConfig.PartialOptions().tap { validateSyncing = overwrite }
        def result = a.merge(b).buildOptions()
        result.validateSyncing == exp

        where:
        base        | overwrite     | exp
        true        | true          | true
        true        | false         | false
        true        | null          | true

        false       | true          | true
        false       | false         | false
        false       | null          | false

        null        | true          | true
        null        | false         | false
        null        | null          | true
    }

    def "Merge options for timeout"() {
        expect:
        def a = new UpstreamsConfig.PartialOptions().tap {
            timeout = base == null ? null : Duration.ofSeconds(base)
        }
        def b = new UpstreamsConfig.PartialOptions().tap {
            timeout = overwrite == null ? null : Duration.ofSeconds(overwrite)
        }
        def result = a.merge(b).buildOptions()
        def expValue = exp == null ? Duration.ofSeconds(60) : Duration.ofSeconds(exp)
        result.timeout == expValue

        where:
        base     | overwrite  | exp
        1        | 2          | 2
        3        | 4          | 4
        5        | null       | 5
        null     | 6          | 6
        null     | null       | null
    }

    def "Merge options for minPeers"() {
        expect:
        def a = new UpstreamsConfig.PartialOptions().tap { minPeers = base }
        def b = new UpstreamsConfig.PartialOptions().tap { minPeers = overwrite }
        def result = a.merge(b).buildOptions()
        result.minPeers == exp

        where:
        base     | overwrite  | exp
        1        | 2          | 2
        3        | 4          | 4
        5        | null       | 5
        null     | 6          | 6
        null     | null       | 1
    }

    def "Merge options for validationInterval"() {
        expect:
        def a = new UpstreamsConfig.PartialOptions().tap { validationInterval = base }
        def b = new UpstreamsConfig.PartialOptions().tap { validationInterval = overwrite }
        def result = a.merge(b).buildOptions()
        result.validationInterval == exp

        where:
        base     | overwrite  | exp
        1        | 2          | 2
        3        | 4          | 4
        5        | null       | 5
        null     | 6          | 6
        null     | null       | 30
    }

    def "Options with default values"() {
        setup:
        def partialOptions = new UpstreamsConfig.PartialOptions()
        when:
        def options = partialOptions.buildOptions()
        then:
        options == new UpstreamsConfig.Options(
                false, 30, Duration.ofSeconds(60), null, true, 1, true
        )
    }
}
