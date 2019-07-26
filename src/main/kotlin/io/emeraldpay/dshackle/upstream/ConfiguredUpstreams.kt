package io.emeraldpay.dshackle.upstream

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.config.UpstreamsConfigReader
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.rpc.DefaultRpcClient
import io.infinitape.etherjar.rpc.transport.DefaultRpcTransport
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.env.Environment
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Repository
import reactor.core.publisher.toFlux
import java.io.File
import java.net.URI
import java.util.*
import javax.annotation.PostConstruct

@Repository
open class ConfiguredUpstreams(
        @Autowired val env: Environment,
        @Autowired private val objectMapper: ObjectMapper
) : Upstreams {

    private val log = LoggerFactory.getLogger(ConfiguredUpstreams::class.java)
    private val chainMapping = HashMap<Chain, ChainUpstreams>()

    private val chainNames = mapOf(
            "ethereum" to Chain.ETHEREUM,
            "ethereum-classic" to Chain.ETHEREUM_CLASSIC,
            "eth" to Chain.ETHEREUM,
            "etc" to Chain.ETHEREUM_CLASSIC,
            "morden" to Chain.TESTNET_MORDEN,
            "kovan" to Chain.TESTNET_KOVAN
    )

    @PostConstruct
    fun start() {
        val config = readConfig()
        val defaultOptions = buildDefaultOptions(config)
        val groups = HashMap<Chain, ArrayList<Upstream>>()
        config.upstreams.forEach { up ->
            if (up.provider == "dshackle") {
                buildGrpcUpstream(up)
            } else {
                buildEthereumUpstream(up, defaultOptions, groups)
            }
        }
        groups.forEach { chain, group ->
            chainMapping[chain] = ChainUpstreams(chain, group)
        }
    }

    private fun readConfig(): UpstreamsConfig {
        val path = env.getProperty("upstreams.config")
        if (StringUtils.isEmpty(path)) {
            log.error("Path to upstreams is not set (upstreams.config)")
            System.exit(1)
        }
        val upstreamConfig = File(path!!)
        val ok = upstreamConfig.exists() && upstreamConfig.isFile
        if (!ok) {
            log.error("Unable to setup upstreams from ${upstreamConfig.path}")
            System.exit(1)
        }
        log.info("Read upstream configuration from ${upstreamConfig.path}")
        val reader = UpstreamsConfigReader()
        return reader.read(upstreamConfig.inputStream())
    }

    private fun buildDefaultOptions(config: UpstreamsConfig): HashMap<Chain, UpstreamsConfig.Options> {
        val defaultOptions = HashMap<Chain, UpstreamsConfig.Options>()
        config.defaultOptions.forEach { df ->
            df.chains.forEach { chainName ->
                chainNames[chainName]?.let { chain ->
                    var current = defaultOptions[chain]
                    if (current == null) {
                        current = df.options
                    } else {
                        current = current.merge(df.options)
                    }
                    defaultOptions[chain] = current
                }
            }
        }
        return defaultOptions
    }

    private fun buildEthereumUpstream(up: UpstreamsConfig.Upstream,
                                      defaultOptions: HashMap<Chain, UpstreamsConfig.Options>,
                                      groups: HashMap<Chain, ArrayList<Upstream>>) {
        val chain = chainNames[up.chain] ?: return
        var rpcApi: EthereumApi? = null
        var wsApi: EthereumWs? = null
        val urls = ArrayList<URI>()
        up.endpoints.forEach { endpoint ->
            if (endpoint.type == UpstreamsConfig.EndpointType.JSON_RPC) {
                rpcApi = EthereumApi(
                        DefaultRpcClient(DefaultRpcTransport(endpoint.url)),
                        objectMapper,
                        chain
                )
            }
            if (endpoint.type == UpstreamsConfig.EndpointType.WEBSOCKET) {
                wsApi = EthereumWs(
                        endpoint.url,
                        endpoint.origin ?: URI("http://localhost")
                )
                wsApi!!.connect()
            }
            urls.add(endpoint.url)
        }
        val options = (up.options ?: UpstreamsConfig.Options())
                .merge(defaultOptions[chain])
                .merge(UpstreamsConfig.Options.getDefaults())
        if (rpcApi != null) {
            log.info("Using ${chain.chainName} upstream, at ${urls.joinToString()}")
            val current = groups[chain] ?: ArrayList()
            current.add(EthereumUpstream(chain, rpcApi!!, wsApi, options))
            groups[chain] = current
        }
    }

    private fun buildGrpcUpstream(up: UpstreamsConfig.Upstream) {
        if (up.endpoints.size == 0) {
            return
        }
        val options = (up.options ?: UpstreamsConfig.Options())
                .merge(UpstreamsConfig.Options.getDefaults())

        val endpoint = up.endpoints.first()
        if (endpoint.type == UpstreamsConfig.EndpointType.DSHACKLE) {
            val ds = GrpcUpstreams(
                    endpoint.host,
                    endpoint.port ?: 443,
                    objectMapper,
                    options
            )
            log.info("Using ALL CHAINS (gRPC) upstream, at ${endpoint.host}:${endpoint.port}")
            ds.start()
                    .flatMapMany {
                        it.toFlux()
                    }
                    .subscribe {
                        log.info("Subscribed to $it through gRPC at ${endpoint.host}:${endpoint.port}")
                        ethereumUpstream(it).addUpstream(ds.getOrCreate(it))
                    }
        }
    }

    override fun ethereumUpstream(chain: Chain): ChainUpstreams {
        val current = chainMapping[chain]
        if (current == null) {
            val created = ChainUpstreams(chain, ArrayList<Upstream>())
            chainMapping[chain] = created
            return created
        }
        return current
    }

    @Scheduled(fixedRate = 15000)
    fun printStatuses() {
        chainMapping.forEach { it.value.printStatus() }
    }

    override fun getAvailable(): List<Chain> {
        return Collections.unmodifiableList(chainMapping.keys.toList())
    }
}