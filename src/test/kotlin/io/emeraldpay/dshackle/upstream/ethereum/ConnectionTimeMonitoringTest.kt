/**
 * Copyright (c) 2023 EmeraldPay, Inc
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

import io.emeraldpay.dshackle.upstream.rpcclient.RpcMetrics
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.comparables.shouldBeGreaterThan
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.mockk.mockk
import io.mockk.verify
import java.time.Duration

class ConnectionTimeMonitoringTest :
    ShouldSpec({

        should("Update time on CONNECT") {
            val monitoring = ConnectionTimeMonitoring(mockk())

            monitoring.connectionTime shouldBe null
            monitoring.accept(WsConnection.ConnectionStatus.CONNECTED)
            Thread.sleep(25)
            monitoring.connectionTime shouldNotBe null
            monitoring.connectionTime!! shouldBeGreaterThan Duration.ofMillis(20)
        }

        should("Record time on DISCONNECT") {
            val rpc = mockk<RpcMetrics>(relaxed = true)
            val monitoring = ConnectionTimeMonitoring(rpc)

            monitoring.accept(WsConnection.ConnectionStatus.CONNECTED)
            Thread.sleep(25)
            monitoring.accept(WsConnection.ConnectionStatus.DISCONNECTED)

            // erases the time after recording it
            monitoring.connectionTime shouldBe null

            verify {
                rpc.recordConnectionTime(more(Duration.ofMillis(20)))
            }
        }
    })
