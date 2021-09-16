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
import java.lang.ClassCastException
import java.net.URI
import java.util.*
import kotlin.collections.ArrayList
import kotlin.collections.HashMap

open class UpstreamsConfig {
    var defaultOptions: MutableList<DefaultOptions> = ArrayList<DefaultOptions>()
    var upstreams: MutableList<Upstream<*>> = ArrayList<Upstream<*>>()

    open class Options {
        var disableValidation: Boolean? = null
        var timeout = Defaults.timeout
        var providesBalance: Boolean? = null

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
            copy.providesBalance = if (this.providesBalance != null) this.providesBalance else additional.providesBalance
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
        var role: UpstreamRole = UpstreamRole.STANDARD

        @Suppress("UNCHECKED_CAST")
        fun <Z : UpstreamConnection> cast(type: Class<Z>): Upstream<Z> {
            if (connection == null || type.isAssignableFrom(connection!!.javaClass)) {
                return this as Upstream<Z>
            }
            throw ClassCastException("Cannot cast ${connection?.javaClass} to $type")
        }
    }

    enum class UpstreamRole {
        STANDARD,
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
    }

    class BitcoinConnection : RpcConnection() {
        var esplora: HttpEndpoint? = null
    }

    class HttpEndpoint(val url: URI) {
        var basicAuth: AuthConfig.ClientBasicAuth? = null
        var tls: AuthConfig.ClientTlsAuth? = null
    }

    class WsEndpoint(val url: URI) {
        var origin: URI? = null
        var basicAuth: AuthConfig.ClientBasicAuth? = null
    }



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
            val quorum: String? = null
    )
}