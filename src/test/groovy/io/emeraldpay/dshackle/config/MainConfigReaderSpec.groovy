/**
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
import io.emeraldpay.grpc.Chain
import spock.lang.Specification

class MainConfigReaderSpec extends Specification {

    MainConfigReader reader = new MainConfigReader(TestingCommons.fileResolver())

    def "Read full config"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("dshackle-full.yaml")
        when:
        def act = reader.read(config)

        then:
        act != null
        act.host == "192.168.1.101"
        act.port == 2448
        act.tls != null
        with(act.tls) {
            certificate == "/path/127.0.0.1.crt"
            key == "/path/127.0.0.1.p8.key"
            !clientRequire
            clientCa == "/path/ca.dshackle.test.crt"
        }
        act.cache != null
        with(act.cache) {
            redis != null
            redis.host == "redis-master"
        }
        act.proxy != null
        with(act.proxy) {
            port == 8082
            tls != null
            routes != null
            routes.size() == 6
            with(routes[0]) {
                id == "eth"
                blockchain == Chain.ETHEREUM
            }
            with(routes[1]) {
                id == "etc"
                blockchain == Chain.ETHEREUM_CLASSIC
            }
            with(routes[2]) {
                id == "kovan"
                blockchain == Chain.TESTNET_KOVAN
            }
            with(routes[3]) {
                id == "goerli"
                blockchain == Chain.TESTNET_GOERLI
            }
            with(routes[4]) {
                id == "rinkeby"
                blockchain == Chain.TESTNET_RINKEBY
            }
            with(routes[5]) {
                id == "bsc"
                blockchain == Chain.BSC
            }
        }
        with(act.health) {
            it.enabled
            with(it.configs()) {
                it.size() == 1
                with(it[0]) {
                    it.blockchain == Chain.ETHEREUM
                    it.minAvailable == 1
                }
            }
        }
        act.upstreams != null
        with(act.upstreams) {
            defaultOptions != null
            defaultOptions.size() == 1
            with(defaultOptions[0]) {
                chains == ["ethereum"]
                options.minPeers == 3
            }
            upstreams.size() == 3
            with(upstreams[0]) {
                id == "remote"
            }
            with(upstreams[1]) {
                id == "local"
            }
            with(upstreams[2]) {
                id == "infura"
            }
        }
    }
}
