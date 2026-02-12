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
import org.apache.commons.lang3.ObjectUtils
import java.net.URI
import java.time.Duration
import java.util.Arrays
import java.util.Locale

open class UpstreamsConfig {
    var defaultOptions: MutableList<DefaultOptions> = ArrayList<DefaultOptions>()
    var upstreams: MutableList<Upstream<*>> = ArrayList<Upstream<*>>()

    companion object {
        private const val MIN_PRIORITY = 0
        private const val MAX_PRIORITY = 1_000_000
        private const val DEFAULT_PRIORITY = 10
        private const val DEFAULT_VALIDATION_INTERVAL = 30
    }

    data class Options(
        var disableValidation: Boolean,
        var validationInterval: Duration,
        var timeout: Duration,
        var providesBalance: Boolean,
        var priority: Int,
        var validatePeers: Boolean,
        var minPeers: Int,
        var validateSyncing: Boolean,
    )

    open class PartialOptions {
        var disableValidation: Boolean? = null
        var validationInterval: Int? = null
            set(value) {
                require(value == null || value > 0) {
                    "validation-interval must be a positive number: $value"
                }
                field = value
            }

        var timeout: Int? = null
        var providesBalance: Boolean? = null
        var priority: Int? = null
            set(value) {
                require(value == null || value in MIN_PRIORITY..MAX_PRIORITY) {
                    "Upstream priority must be in $MIN_PRIORITY..$MAX_PRIORITY. Configured: $value"
                }
                field = value
            }
        var validatePeers: Boolean? = null
        var minPeers: Int? = null
            set(value) {
                require(value == null || value >= 0) {
                    "min-peers must be a positive number: $value"
                }
                field = value
            }
        var validateSyncing: Boolean? = null

        fun merge(overwrites: PartialOptions?): PartialOptions {
            if (overwrites == null) {
                return this
            }
            val copy = PartialOptions()
            copy.disableValidation = ObjectUtils.firstNonNull(overwrites.disableValidation, this.disableValidation)
            copy.validationInterval = ObjectUtils.firstNonNull(overwrites.validationInterval, this.validationInterval)
            copy.timeout = ObjectUtils.firstNonNull(overwrites.timeout, this.timeout)
            copy.providesBalance = ObjectUtils.firstNonNull(overwrites.providesBalance, this.providesBalance)
            copy.priority = ObjectUtils.firstNonNull(overwrites.priority, this.priority)
            copy.validatePeers = ObjectUtils.firstNonNull(overwrites.validatePeers, this.validatePeers)
            copy.minPeers = ObjectUtils.firstNonNull(overwrites.minPeers, this.minPeers)
            copy.validateSyncing = ObjectUtils.firstNonNull(overwrites.validateSyncing, this.validateSyncing)
            return copy
        }

        fun build(): Options =
            Options(
                disableValidation = ObjectUtils.firstNonNull(this.disableValidation, false)!!,
                validationInterval =
                    ObjectUtils
                        .firstNonNull(this.validationInterval, DEFAULT_VALIDATION_INTERVAL)!!
                        .toLong()
                        .let(Duration::ofSeconds),
                timeout = ObjectUtils.firstNonNull(this.timeout?.toLong()?.let(Duration::ofSeconds), Defaults.timeout)!!,
                providesBalance = ObjectUtils.firstNonNull(this.providesBalance, false)!!,
                priority = ObjectUtils.firstNonNull(this.priority, DEFAULT_PRIORITY)!!,
                validatePeers = ObjectUtils.firstNonNull(this.validatePeers, true)!!,
                minPeers = ObjectUtils.firstNonNull(this.minPeers, 1)!!,
                validateSyncing = ObjectUtils.firstNonNull(this.validateSyncing, true)!!,
            )

        companion object {
            @JvmStatic
            fun getDefaults(): PartialOptions {
                val options = PartialOptions()
                options.minPeers = 1
                options.disableValidation = false
                return options
            }
        }
    }

    class DefaultOptions : PartialOptions() {
        var blockchains: List<String>? = null
        var options: PartialOptions? = null
    }

    class Upstream<T : UpstreamConnection> {
        var id: String? = null
        var blockchain: String? = null
        var options: PartialOptions? = null
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
        FALLBACK,
    }

    open class UpstreamConnection

    open class RpcConnection : UpstreamConnection() {
        var rpc: HttpEndpoint? = null
    }

    class GrpcConnection : UpstreamConnection() {
        var host: String? = null
        var port: Int = 0
        var auth: AuthConfig.ClientTlsAuth? = null
        var autoTls: Boolean? = null
        var compress: Boolean? = null
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
        val port: Int,
        val topics: List<String>,
    )

    class HttpEndpoint(
        val url: URI,
    ) {
        var basicAuth: AuthConfig.ClientBasicAuth? = null
        var tls: AuthConfig.ClientTlsAuth? = null
        var compress: Boolean? = null
    }

    class WsEndpoint(
        val url: URI,
    ) {
        var origin: URI? = null
        var basicAuth: AuthConfig.ClientBasicAuth? = null
        var frameSize: Int? = null
        var msgSize: Int? = null
        var connections: Int = 1
        var compress: Boolean? = null
        var disabledMethods: List<String> = emptyList()
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

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is Labels) return false

            return this.size == other.size && this.entries == other.entries
        }

        override fun toString(): String = "[" + this.entries.joinToString(", ") { "${it.key}=${it.value}" } + "]"
    }

    enum class UpstreamType(
        vararg code: String,
    ) {
        ETHEREUM_JSON_RPC("ethereum"),
        BITCOIN_JSON_RPC("bitcoin"),
        DSHACKLE("dshackle", "grpc"),
        UNKNOWN("unknown"),
        ;

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

    class Method(
        val name: String,
        val quorum: String? = null,
        val static: String? = null,
    )
}
