package io.emeraldpay.dshackle.config

import java.net.URI
import java.util.*
import kotlin.collections.ArrayList
import kotlin.collections.HashMap

class UpstreamsConfig {
    var version: String? = null
    var defaultOptions: MutableList<DefaultOptions> = ArrayList<DefaultOptions>()
    var upstreams: MutableList<Upstream<*>> = ArrayList<Upstream<*>>()

    open class Options {
        @Deprecated("remove it")
        var quorum: Int = 1
        var disableSyncing: Boolean? = null
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
            copy.disableSyncing = if (this.disableSyncing != null) this.disableSyncing else additional.disableSyncing
            copy.minPeers = if (this.minPeers != null) this.minPeers else additional.minPeers
            return copy
        }

        companion object {
            fun getDefaults(): Options {
                val options = Options()
                options.disableSyncing = true
                options.minPeers = 1
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
        var auth: Auth? = null
    }

    class WsEndpoint(val url: URI) {
        var origin: URI? = null
    }

    open class Auth {
        var type: String? = null
    }

    class BasicAuth : Auth() {
        var key: String? = null
    }

    class TlsAuth : Auth() {
        var ca: String? = null
        var certificate: String? = null
        var key: String? = null
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
}