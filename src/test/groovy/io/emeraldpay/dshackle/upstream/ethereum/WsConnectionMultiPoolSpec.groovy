/**
 * Copyright (c) 2022 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.upstream.DefaultUpstream
import reactor.core.publisher.Flux
import spock.lang.Specification

import java.util.concurrent.ScheduledExecutorService

class WsConnectionMultiPoolSpec extends Specification {

    def "create connection when less than required"() {
        setup:
        def conn = Mock(WsConnection) {
            1 * it.connectionInfoFlux() >> Flux.empty()
        }
        def up = Mock(DefaultUpstream)
        def factory = Mock(EthereumWsConnectionFactory)
        def pool = new WsConnectionMultiPool(factory, up, 3)
        pool.scheduler = Stub(ScheduledExecutorService)

        when:
        pool.connect()

        then:
        1 * factory.createWsConnection(0, _) >> conn
        1 * conn.connect()
    }

    def "create connection until target"() {
        setup:
        def conn1 = Mock(WsConnection) {
            1 * it.connectionInfoFlux() >> Flux.empty()
        }
        def conn2 = Mock(WsConnection) {
            1 * it.connectionInfoFlux() >> Flux.empty()
        }
        def conn3 = Mock(WsConnection) {
            1 * it.connectionInfoFlux() >> Flux.empty()
        }
        def up = Mock(DefaultUpstream)
        def factory = Mock(EthereumWsConnectionFactory)
        def pool = new WsConnectionMultiPool(factory, up, 3)
        pool.scheduler = Stub(ScheduledExecutorService)

        when:
        pool.connect()

        then:
        1 * factory.createWsConnection(0, _) >> conn1
        1 * conn1.connect()

        when:
        pool.connect()

        then:
        1 * conn1.isConnected() >> true
        1 * factory.createWsConnection(1, _) >> conn2
        1 * conn2.connect()

        when:
        pool.connect()

        then:
        1 * conn1.isConnected() >> true
        1 * conn2.isConnected() >> true
        1 * factory.createWsConnection(2, _) >> conn3
        1 * conn3.connect()

        when:
        pool.connect()

        then:
        1 * conn1.isConnected() >> true
        1 * conn2.isConnected() >> true
        1 * conn3.isConnected() >> true
        0 * factory.createWsConnection(_, _)
    }

    def "recreate connection after failure"() {
        setup:
        def conn1 = Mock(WsConnection) {
            1 * it.connectionInfoFlux() >> Flux.empty()
        }
        def conn2 = Mock(WsConnection) {
            1 * it.connectionInfoFlux() >> Flux.empty()
        }
        def conn3 = Mock(WsConnection) {
            1 * it.connectionInfoFlux() >> Flux.empty()
        }
        def conn4 = Mock(WsConnection) {
            1 * it.connectionInfoFlux() >> Flux.empty()
        }
        def up = Mock(DefaultUpstream)
        def factory = Mock(EthereumWsConnectionFactory)
        def pool = new WsConnectionMultiPool(factory, up, 3)
        pool.scheduler = Stub(ScheduledExecutorService)

        when: "initial fill"
        pool.connect()
        pool.connect()
        pool.connect()

        then:
        _ * conn1.isConnected() >> true
        _ * conn2.isConnected() >> true
        _ * conn3.isConnected() >> true
        1 * factory.createWsConnection(0, _) >> conn1
        1 * factory.createWsConnection(1, _) >> conn2
        1 * factory.createWsConnection(2, _) >> conn3
        1 * conn1.connect()
        1 * conn2.connect()
        1 * conn3.connect()

        when: "all ok"
        pool.connect()

        then:
        1 * conn1.isConnected() >> true
        1 * conn2.isConnected() >> true
        1 * conn3.isConnected() >> true
        0 * factory.createWsConnection(_, _)

        when: "one failed"
        pool.connect()

        then:
        (1.._) * conn1.isConnected() >> true
        (1.._) * conn2.isConnected() >> false
        (1.._) * conn3.isConnected() >> true
        0 * factory.createWsConnection(_, _) // doesn't create immediately, but schedules it for the next adjust
        1 * conn2.close()

        when: "needs one more"
        pool.connect()

        then:
        1 * conn1.isConnected() >> true
        1 * conn3.isConnected() >> true
        1 * factory.createWsConnection(3, _) >> conn4
        1 * conn4.connect()
    }
}
