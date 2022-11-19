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

import spock.lang.Specification

import java.util.concurrent.ScheduledExecutorService

class WsConnectionMultiPoolSpec extends Specification {

    def "create connection when less than required"() {
        setup:
        def conn = Mock(WsConnection)
        def factory = Mock(EthereumWsFactory)
        def pool = new WsConnectionMultiPool(factory, 3)
        pool.scheduler = Stub(ScheduledExecutorService)

        when:
        pool.adjust()

        then:
        1 * factory.create(_) >> conn
        1 * conn.connect()
    }

    def "create connection until target"() {
        setup:
        def conn1 = Mock(WsConnection)
        def conn2 = Mock(WsConnection)
        def conn3 = Mock(WsConnection)
        def factory = Mock(EthereumWsFactory)
        def pool = new WsConnectionMultiPool(factory, 3)
        pool.scheduler = Stub(ScheduledExecutorService)

        when:
        pool.adjust()

        then:
        1 * factory.create(_) >> conn1
        1 * conn1.connect()

        when:
        pool.adjust()

        then:
        1 * conn1.isConnected() >> true
        1 * factory.create(_) >> conn2
        1 * conn2.connect()

        when:
        pool.adjust()

        then:
        1 * conn1.isConnected() >> true
        1 * conn2.isConnected() >> true
        1 * factory.create(_) >> conn3
        1 * conn3.connect()

        when:
        pool.adjust()

        then:
        1 * conn1.isConnected() >> true
        1 * conn2.isConnected() >> true
        1 * conn3.isConnected() >> true
        0 * factory.create(_)
    }

    def "recreate connection after failure"() {
        setup:
        def conn1 = Mock(WsConnection)
        def conn2 = Mock(WsConnection)
        def conn3 = Mock(WsConnection)
        def conn4 = Mock(WsConnection)
        def factory = Mock(EthereumWsFactory)
        def pool = new WsConnectionMultiPool(factory, 3)
        pool.scheduler = Stub(ScheduledExecutorService)

        when: "initial fill"
        pool.adjust()
        pool.adjust()
        pool.adjust()

        then:
        _ * conn1.isConnected() >> true
        _ * conn2.isConnected() >> true
        _ * conn3.isConnected() >> true
        3 * factory.create(_) >>> [conn1, conn2, conn3]
        1 * conn1.connect()
        1 * conn2.connect()
        1 * conn3.connect()

        when: "all ok"
        pool.adjust()

        then:
        1 * conn1.isConnected() >> true
        1 * conn2.isConnected() >> true
        1 * conn3.isConnected() >> true
        0 * factory.create(_)

        when: "one failed"
        pool.adjust()

        then:
        (1.._) * conn1.isConnected() >> true
        (1.._) * conn2.isConnected() >> false
        (1.._) * conn3.isConnected() >> true
        0 * factory.create(_) // doesn't create immediately, but schedules it for the next adjust
        1 * conn2.close()

        when: "needs one more"
        pool.adjust()

        then:
        1 * conn1.isConnected() >> true
        1 * conn3.isConnected() >> true
        1 * factory.create(_) >> conn4
        1 * conn4.connect()
    }
}
