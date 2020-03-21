/**
 * Copyright (c) 2020 ETCDEV GmbH
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

import io.emeraldpay.grpc.Chain

/**
 * Configure HTTP Proxy to Upstreams
 */
class ProxyConfig {

    companion object {
        public const val CONFIG_ID = "parsed.proxy"
    }

    var enabled: Boolean = true

    /**
     * Host to bind server. Default: 127.0.0.1
     */
    var host = "127.0.0.1"

    /**
     * Port to bind. Default: 8080
     */
    var port: Int = 8080

    /**
     * TLS Auth required from clients.
     */
    var tls: AuthConfig.ServerTlsAuth? = null

    /**
     * List of available routes
     */
    var routes: List<Route> = ArrayList()

    class Route(
            /**
             * URL binding for the route. http://$host:$port/$id
             */
            val id: String,
            /**
             * Blockchain to dispatch requests
             */
            val blockchain: Chain
    )
}