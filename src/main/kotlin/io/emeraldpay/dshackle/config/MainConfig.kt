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

class MainConfig {
    var host = "127.0.0.1"
    var port = 2449
    var tls: AuthConfig.ServerTlsAuth? = null
    var passthrough: Boolean = false
    var cache: CacheConfig? = null
    var index: IndexConfig? = null
    var proxy: ProxyConfig? = null
    var upstreams: UpstreamsConfig? = null
    var tokens: TokensConfig? = null
    var monitoring: MonitoringConfig = MonitoringConfig.default()
    var accessLogConfig: AccessLogConfig = AccessLogConfig.default()
    var health: HealthConfig = HealthConfig.default()
    var signature: SignatureConfig? = null
    var compression: CompressionConfig = CompressionConfig.default()
    var chains: ChainsConfig = ChainsConfig.default()
    var authorization: AuthorizationConfig = AuthorizationConfig.default()
}
