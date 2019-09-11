/**
 * Copyright (c) 2019 ETCDEV GmbH
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

import io.emeraldpay.dshackle.Defaults
import java.net.URI
import java.time.Duration
import java.util.*
import kotlin.collections.ArrayList
import kotlin.collections.HashMap

class UpstreamsConfig {
    var version: String? = null
    var defaultOptions: MutableList<DefaultOptions> = ArrayList<DefaultOptions>()
    var upstreams: MutableList<Upstream<*>> = ArrayList<Upstream<*>>()

    open class Options {
        var disableValidation: Boolean? = null
        var timeout = Defaults.timeout

        var minPeers: Int? = 1
            set(minPeers) {
                if (minPeers != null && minPeers < 0) {
                    throw IllegalArgumentException("minPeers must be positive number")
                }
                field = minPeers
            }

        fun merge(additional: Options?): Options {
            if (additional == null) {
                return this
            }
            val copy = Options()
            copy.minPeers = if (this.minPeers != null) this.minPeers else additional.minPeers
            copy.disableValidation = if (this.disableValidation != null) this.disableValidation else additional.disableValidation
            return copy
        }

        companion object {
            @JvmStatic
            fun getDefaults(): Options {
                val options = Options()
                options.minPeers = 1
                options.disableValidation = false
                return options
            }
        }

    }

    class DefaultOptions : Options() {
        var chains: List<String>? = null
        var options: Options? = null
    }

    class Upstream<T : UpstreamConnection> {
        var id: String? = null
        var chain: String? = null
        var options: Options? = null
        var isEnabled = true
        var connection: T? = null
        val labels = Labels()
        var methods: Methods? = null
    }

    open class UpstreamConnection

    class GrpcConnection : UpstreamConnection() {
        var host: String? = null
        var port: Int = 0
        var auth: TlsAuth? = null
    }

    class EthereumConnection : UpstreamConnection() {
        var rpc: HttpEndpoint? = null
        var ws: WsEndpoint? = null
    }

    class HttpEndpoint(val url: URI) {
        var basicAuth: BasicAuth? = null
        var tls: TlsAuth? = null
    }

    class WsEndpoint(val url: URI) {
        var origin: URI? = null
        var basicAuth: BasicAuth? = null
    }

    open class Auth {
        var type: String? = null
    }

    class BasicAuth(
            val username: String,
            val password: String
    ) : Auth()

    class TlsAuth(
        var ca: String? = null,
        var certificate: String? = null,
        var key: String? = null
    ) : Auth()

    //TODO make it unmodifiable after initial load
    class Labels: HashMap<String, String>() {

        companion object {
            @JvmStatic fun fromMap(map: Map<String, String>): Labels {
                val labels = Labels()
                map.entries.forEach() { kv ->
                    labels.put(kv.key, kv.value)
                }
                return labels
            }
        }
    }

    enum class UpstreamType private constructor(vararg code: String) {
        ETHEREUM_JSON_RPC("ethereum"),
        DSHACKLE("dshackle", "grpc"),
        UNKNOWN("unknown");

        private val code: Array<String>

        init {
            this.code = code as Array<String>
            Arrays.sort(this.code)
        }

        companion object {

            fun byName(code: String): UpstreamType {
                var code = code
                code = code.toLowerCase()
                for (t in UpstreamType.values()) {
                    if (Arrays.binarySearch(t.code, code) >= 0) {
                        return t
                    }
                }
                return UNKNOWN
            }
        }
    }

    class Methods(
            val enabled: Set<Method>,
            val disabled: Set<Method>
    )

    class Method(
            val name: String
    )
}