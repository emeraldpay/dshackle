/**
 * Copyright (c) 2020 EmeraldPay, Inc
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
import java.util.Arrays
import java.util.Locale

open class UpstreamsConfig {
    var defaultOptions: MutableList<DefaultOptions> = ArrayList<DefaultOptions>()
    var upstreams: MutableList<Upstream<*>> = ArrayList<Upstream<*>>()

    companion object {
        private const val MIN_PRIORITY = 0
        private const val MAX_PRIORITY = 1_000_000
        private const val DEFAULT_PRIORITY = 10
    }

    open class Options {
        var disableValidation: Boolean? = null
        var validationInterval: Int = 30
            set(value) {
                require(value > 0) {
                    "validation-interval must be a positive number: $value"
                }
                field = value
            }
        var timeout = Defaults.timeout
        var providesBalance: Boolean? = null
        var priority: Int = DEFAULT_PRIORITY
            set(value) {
                require(value in MIN_PRIORITY..MAX_PRIORITY) {
                    "Upstream priority must be in $MIN_PRIORITY..$MAX_PRIORITY. Configured: $value"
                }
                field = value
            }
        var validatePeers: Boolean = true
        var minPeers: Int? = 1
            set(value) {
                require(value != null && value >= 0) {
                    "min-peers must be a positive number: $value"
                }
                field = value
            }
        var validateSyncing: Boolean = true

        fun merge(overwrites: Options?): Options {
            if (overwrites == null) {
                return this
            }
            val copy = Options()
            copy.priority = this.priority.coerceAtLeast(overwrites.priority)
            copy.validatePeers = this.validatePeers && overwrites.validatePeers
            copy.minPeers = if (this.minPeers != null) this.minPeers else overwrites.minPeers
            copy.disableValidation =
                if (this.disableValidation != null) this.disableValidation else overwrites.disableValidation
            copy.validationInterval = overwrites.validationInterval
            copy.providesBalance =
                if (this.providesBalance != null) this.providesBalance else overwrites.providesBalance
            copy.validateSyncing = this.validateSyncing && overwrites.validateSyncing
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
        var role: UpstreamRole = UpstreamRole.PRIMARY

        @Suppress("UNCHECKED_CAST")
        fun <Z : UpstreamConnection> cast(type: Class<Z>): Upstream<Z> {
            if (connection == null || type.isAssignableFrom(connection!!.javaClass)) {
                return this as Upstream<Z>
            }
            throw ClassCastException("Cannot cast ${connection?.javaClass} to $type")
        }
    }

    enum class UpstreamRole {
        PRIMARY,
        SECONDARY,
        FALLBACK
    }

    open class UpstreamConnection

    open class RpcConnection : UpstreamConnection() {
        var rpc: HttpEndpoint? = null
    }

    class GrpcConnection : UpstreamConnection() {
        var host: String? = null
        var port: Int = 0
        var auth: AuthConfig.ClientTlsAuth? = null
    }

    class EthereumConnection : RpcConnection() {
        var ws: WsEndpoint? = null
        var preferHttp: Boolean = false
    }

    class BitcoinConnection : RpcConnection() {
        var esplora: HttpEndpoint? = null
        var zeroMq: BitcoinZeroMq? = null
    }

    data class BitcoinZeroMq(
        val host: String = "127.0.0.1",
        val port: Int
    )

    class HttpEndpoint(val url: URI) {
        var basicAuth: AuthConfig.ClientBasicAuth? = null
        var tls: AuthConfig.ClientTlsAuth? = null
    }

    class WsEndpoint(val url: URI) {
        var origin: URI? = null
        var basicAuth: AuthConfig.ClientBasicAuth? = null
        var frameSize: Int? = null
        var msgSize: Int? = null
    }

    // TODO make it unmodifiable after initial load
    class Labels : HashMap<String, String>() {

        companion object {
            @JvmStatic
            fun fromMap(map: Map<String, String>): Labels {
                val labels = Labels()
                map.entries.forEach { kv ->
                    labels.put(kv.key, kv.value)
                }
                return labels
            }
        }
    }

    enum class UpstreamType(vararg code: String) {
        ETHEREUM_JSON_RPC("ethereum"),
        BITCOIN_JSON_RPC("bitcoin"),
        DSHACKLE("dshackle", "grpc"),
        UNKNOWN("unknown");

        private val code: Array<out String>

        init {
            this.code = code
            Arrays.sort(this.code)
        }

        companion object {

            fun byName(code: String): UpstreamType {
                val cleanCode = code.lowercase(Locale.getDefault())
                for (t in UpstreamType.values()) {
                    if (Arrays.binarySearch(t.code, cleanCode) >= 0) {
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
        val name: String,
        val quorum: String? = null,
        val static: String? = null
    )
}
