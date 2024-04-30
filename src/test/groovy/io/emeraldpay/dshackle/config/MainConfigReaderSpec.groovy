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

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.test.TestingCommons
import spock.lang.Specification

class MainConfigReaderSpec extends Specification {

    def "Read full config with inclusion"() {
        setup:
        // the File Resolver should be able to resolve/include files
        MainConfigReader reader = new MainConfigReader(TestingCommons.fileResolver())
        // note that it references another config to be included
        def config = this.class.getClassLoader().getResourceAsStream("configs/dshackle-full.yaml")
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
            clientCAs == ["/path/ca.dshackle.test.crt"]
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
            routes.size() == 2
            with(routes[0]) {
                id == "eth"
                blockchain == Chain.ETHEREUM__MAINNET
            }
            with(routes[1]) {
                id == "etc"
                blockchain == Chain.ETHEREUM_CLASSIC__MAINNET
            }
        }
        with(act.health) {
            it.enabled
            with(it.configs()) {
                it.size() == 1
                with(it[0]) {
                    it.blockchain == Chain.ETHEREUM__MAINNET
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
            upstreams.size() == 4
            with(upstreams[0]) {
                id == "remote"
            }
            with(upstreams[1]) {
                id == "remoteTokenAuth"
                connection instanceof UpstreamsConfig.GrpcConnection
                with(connection as UpstreamsConfig.GrpcConnection) {
                    it.tokenAuth.publicKeyPath == "/path/to/key.pem"
                }
            }
            with(upstreams[2]) {
                id == "local"
            }
            with(upstreams[3]) {
                id == "infura"
            }
        }
        act.authorization != null
        with(act.authorization) {
            enabled
            serverConfig.providerPrivateKeyPath == "classpath:keys/priv.p8.key"
            serverConfig.externalPublicKeyPath == "classpath:keys/public.pem"
        }
    }
}
