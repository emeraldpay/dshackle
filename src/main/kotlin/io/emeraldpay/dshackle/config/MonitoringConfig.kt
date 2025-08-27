/**
 * Copyright (c) 2021 EmeraldPay, Inc
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

class MonitoringConfig(
    val enabled: Boolean,
    val prometheus: PrometheusConfig,
) {
    companion object {
        fun default(): MonitoringConfig = MonitoringConfig(true, PrometheusConfig.default())

        fun disabled(): MonitoringConfig = MonitoringConfig(false, PrometheusConfig.disabled())
    }

    var enableJvm: Boolean = true
    var enableExtended: Boolean = false

    data class PrometheusConfig(
        val enabled: Boolean,
        val path: String,
        val host: String,
        val port: Int,
    ) {
        companion object {
            fun default(): PrometheusConfig = PrometheusConfig(true, "/metrics", "127.0.0.1", 8081)

            fun disabled(): PrometheusConfig = PrometheusConfig(false, "/", "127.0.0.1", 0)
        }
    }
}
