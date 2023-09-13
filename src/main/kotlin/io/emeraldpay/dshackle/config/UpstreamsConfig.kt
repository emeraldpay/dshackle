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
import io.emeraldpay.dshackle.upstream.ethereum.connectors.EthereumConnectorFactory.ConnectorMode
import org.apache.commons.lang3.ObjectUtils.firstNonNull
import java.net.URI
import java.time.Duration
import java.util.Arrays
import java.util.Locale
import java.util.concurrent.ConcurrentHashMap

open class UpstreamsConfig {
    var defaultOptions: MutableList<DefaultOptions> = ArrayList<DefaultOptions>()
    var upstreams: MutableList<Upstream<*>> = ArrayList<Upstream<*>>()

    data class Options(
        val disableUpstreamValidation: Boolean,
        val disableValidation: Boolean,
        val validationInterval: Int,
        val timeout: Duration,
        val providesBalance: Boolean?,
        val validatePeers: Boolean,
        val minPeers: Int,
        val validateSyncing: Boolean,
        val validateCallLimit: Boolean,
        val validateChain: Boolean
    )

    open class PartialOptions {
        var disableValidation: Boolean? = null
        var disableUpstreamValidation: Boolean? = null
        var validationInterval: Int? = null
            set(value) {
                require(value == null || value > 0) {
                    "validation-interval must be a positive number: $value"
                }
                field = value
            }
        var timeout: Duration? = null
        var providesBalance: Boolean? = null
        var validatePeers: Boolean? = null
        var validateCalllimit: Boolean? = null
        var minPeers: Int? = null
            set(value) {
                require(value == null || value >= 0) {
                    "min-peers must be a positive number: $value"
                }
                field = value
            }
        var validateSyncing: Boolean? = null
        var validateChain: Boolean? = null

        fun merge(overwrites: PartialOptions?): PartialOptions {
            if (overwrites == null) {
                return this
            }
            val copy = PartialOptions()
            copy.validatePeers = firstNonNull(overwrites.validatePeers, this.validatePeers)
            copy.minPeers = firstNonNull(overwrites.minPeers, this.minPeers)
            copy.disableValidation = firstNonNull(overwrites.disableValidation, this.disableValidation)
            copy.validationInterval = firstNonNull(overwrites.validationInterval, this.validationInterval)
            copy.providesBalance = firstNonNull(overwrites.providesBalance, this.providesBalance)
            copy.validateSyncing = firstNonNull(overwrites.validateSyncing, this.validateSyncing)
            copy.validateCalllimit = firstNonNull(overwrites.validateCalllimit, this.validateCalllimit)
            copy.timeout = firstNonNull(overwrites.timeout, this.timeout)
            copy.validateChain = firstNonNull(overwrites.validateChain, this.validateChain)
            copy.disableUpstreamValidation = firstNonNull(overwrites.disableUpstreamValidation, this.disableUpstreamValidation)
            return copy
        }

        fun buildOptions(): Options =
            Options(
                firstNonNull(this.disableUpstreamValidation, false)!!,
                firstNonNull(this.disableValidation, false)!!,
                firstNonNull(this.validationInterval, 30)!!,
                firstNonNull(this.timeout, Defaults.timeout)!!,
                this.providesBalance,
                firstNonNull(this.validatePeers, true)!!,
                firstNonNull(this.minPeers, 1)!!,
                firstNonNull(this.validateSyncing, true)!!,
                firstNonNull(this.validateCalllimit, true)!!,
                firstNonNull(this.validateChain, true)!!
            )

        companion object {
            @JvmStatic
            fun getDefaults(): PartialOptions {
                val options = PartialOptions()
                options.minPeers = 1
                return options
            }
        }
    }

    class DefaultOptions : PartialOptions() {
        var chains: List<String>? = null
        var options: PartialOptions? = null
    }

    class Upstream<T : UpstreamConnection> {
        var id: String? = null
        var nodeId: Int? = null
        var chain: String? = null
        var options: PartialOptions? = null
        var isEnabled = true
        var connection: T? = null
        val labels = Labels()
        var methods: Methods? = null
        var methodGroups: MethodGroups? = null
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
        var tokenAuth: AuthConfig.ClientTokenAuth? = null
        var upstreamRating: Int = 0
    }

    class EthereumConnection : RpcConnection() {
        var ws: WsEndpoint? = null
        var connectorMode: String? = null

        fun resolveMode(): ConnectorMode {
            return if (connectorMode == null) {
                if (ws != null && rpc != null) {
                    ConnectorMode.RPC_REQUESTS_WITH_WS_HEAD
                } else if (ws == null) {
                    ConnectorMode.RPC_ONLY
                } else {
                    ConnectorMode.WS_ONLY
                }
            } else {
                ConnectorMode.parse(connectorMode!!)
            }
        }
    }

    class BitcoinConnection : RpcConnection() {
        var esplora: HttpEndpoint? = null
        var zeroMq: BitcoinZeroMq? = null
    }

    class EthereumPosConnection : UpstreamConnection() {
        var execution: EthereumConnection? = null
        var upstreamRating: Int = 0
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
        var connections: Int = 1
    }

    // TODO make it unmodifiable after initial load
    class Labels : ConcurrentHashMap<String, String>() {

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
        val disabled: Set<Method>,
    )

    class MethodGroups(
        val enabled: Set<String>,
        val disabled: Set<String>,
    )

    class Method(
        val name: String,
        val quorum: String? = null,
        val static: String? = null
    )
}
