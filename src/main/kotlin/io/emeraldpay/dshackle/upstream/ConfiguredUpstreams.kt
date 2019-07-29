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
        config.upstreams.forEach { up ->
            val options = (up.options ?: UpstreamsConfig.Options())
                    .merge(UpstreamsConfig.Options.getDefaults())

            if (up.provider == "dshackle") {
                buildGrpcUpstream(up.connection as UpstreamsConfig.GrpcConnection, options)
            } else {
                val chain = chainNames[up.chain] ?: return
                buildEthereumUpstream(up.connection as UpstreamsConfig.EthereumConnection, chain, options)
            }
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
            df.chains?.forEach { chainName ->
                chainNames[chainName]?.let { chain ->
                    var current = defaultOptions[chain]
                    if (current == null) {
                        current = df.options
                    } else {
                        current = current.merge(df.options)
                    }
                    defaultOptions[chain] = current!!
                }
            }
        }
        return defaultOptions
    }

    private fun buildEthereumUpstream(up: UpstreamsConfig.EthereumConnection,
                                      chain: Chain,
                                      options: UpstreamsConfig.Options) {
        var rpcApi: EthereumApi? = null
        var wsApi: EthereumWs? = null
        val urls = ArrayList<URI>()
        up.rpc?.let { endpoint ->
            rpcApi = EthereumApi(
                    DefaultRpcClient(DefaultRpcTransport(endpoint.url)),
                    objectMapper,
                    chain
            )
            urls.add(endpoint.url)
        }
        up.ws?.let { endpoint ->
            wsApi = EthereumWs(
                    endpoint.url,
                    endpoint.origin ?: URI("http://localhost")
            )
            wsApi!!.connect()
            urls.add(endpoint.url)
        }
        if (rpcApi != null) {
            log.info("Using ${chain.chainName} upstream, at ${urls.joinToString()}")
            getOrCreateUpstream(chain).addUpstream(EthereumUpstream(chain, rpcApi!!, wsApi, options))
        }
    }

    private fun buildGrpcUpstream(up: UpstreamsConfig.GrpcConnection, options: UpstreamsConfig.Options) {
        val endpoint = up
            val ds = GrpcUpstreams(
                    endpoint.host!!,
                    endpoint.port ?: 443,
                    objectMapper,
                    options,
                    up.auth
            )
            log.info("Using ALL CHAINS (gRPC) upstream, at ${endpoint.host}:${endpoint.port}")
            ds.start()
                    .flatMapMany {
                        it.toFlux()
                    }
                    .subscribe {
                        log.info("Subscribed to $it through gRPC at ${endpoint.host}:${endpoint.port}")
                        getOrCreateUpstream(it).addUpstream(ds.getOrCreate(it))
                    }
    }

    override fun getUpstream(chain: Chain): AggregatedUpstreams? {
        return chainMapping[chain]
    }

    override fun getOrCreateUpstream(chain: Chain): ChainUpstreams {
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