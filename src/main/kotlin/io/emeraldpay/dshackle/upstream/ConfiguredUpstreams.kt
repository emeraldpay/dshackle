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
import reactor.core.publisher.Flux
import reactor.core.publisher.TopicProcessor
import reactor.core.publisher.toFlux
import java.io.File
import java.net.URI
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import javax.annotation.PostConstruct
import kotlin.collections.HashMap

@Repository
open class ConfiguredUpstreams(
        @Autowired val env: Environment,
        @Autowired private val objectMapper: ObjectMapper
) : Upstreams {

    private val log = LoggerFactory.getLogger(ConfiguredUpstreams::class.java)
    private val chainMapping = ConcurrentHashMap<Chain, ChainUpstreams>()
    private val chainsBus = TopicProcessor.create<Chain>()
    private val callTargets = HashMap<Chain, QuorumBasedMethods>()

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

            if (up.connection is UpstreamsConfig.GrpcConnection) {
                buildGrpcUpstream(up.connection as UpstreamsConfig.GrpcConnection)
            } else {
                val chain = chainNames[up.chain]
                if (chain == null) {
                    log.error("Chain not supported: ${up.chain}")
                    return@forEach
                }
                val options = (up.options ?: UpstreamsConfig.Options())
                        .merge(defaultOptions[chain] ?: UpstreamsConfig.Options.getDefaults())
                buildEthereumUpstream(up.connection as UpstreamsConfig.EthereumConnection, chain, options, up.labels)
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
        config.defaultOptions.forEach { defaultsConfig ->
            defaultsConfig.chains?.forEach { chainName ->
                chainNames[chainName]?.let { chain ->
                    defaultsConfig.options?.let { options ->
                        if (!defaultOptions.containsKey(chain)) {
                            defaultOptions[chain] = options
                        } else {
                            defaultOptions[chain] = defaultOptions[chain]!!.merge(options)
                        }
                    }
                }
            }
        }
        defaultOptions.keys.forEach { chain ->
            defaultOptions[chain] = defaultOptions[chain]!!.merge(UpstreamsConfig.Options.getDefaults())
        }
        return defaultOptions
    }

    private fun buildEthereumUpstream(up: UpstreamsConfig.EthereumConnection,
                                      chain: Chain,
                                      options: UpstreamsConfig.Options,
                                      labels: UpstreamsConfig.Labels) {
        var rpcApi: EthereumApi? = null
        var wsApi: EthereumWs? = null
        val urls = ArrayList<URI>()
        up.rpc?.let { endpoint ->
            val rpcTransport = DefaultRpcTransport(endpoint.url)
            up.auth?.let { auth ->
                rpcTransport.setBasicAuth(auth.username, auth.password)
            }
            val rpcClient = DefaultRpcClient(rpcTransport)
            rpcApi = EthereumApi(
                    rpcClient,
                    objectMapper,
                    chain,
                    targetFor(chain)
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
            val ethereumUpstream = EthereumUpstream(chain, rpcApi!!, wsApi, options, NodeDetailsList.NodeDetails(1, labels), targetFor(chain))
            ethereumUpstream.start()
            addUpstream(chain, ethereumUpstream)
        }
    }

    private fun buildGrpcUpstream(up: UpstreamsConfig.GrpcConnection) {
        val endpoint = up
            val ds = GrpcUpstreams(
                    endpoint.host!!,
                    endpoint.port ?: 443,
                    objectMapper,
                    up.auth,
                    this
            )
            log.info("Using ALL CHAINS (gRPC) upstream, at ${endpoint.host}:${endpoint.port}")
            ds.start()
                    .subscribe {
                        log.info("Subscribed to ${it.t1} through gRPC at ${endpoint.host}:${endpoint.port}")
                        addUpstream(it.t1, it.t2)
                    }
    }

    override fun getUpstream(chain: Chain): AggregatedUpstream? {
        return chainMapping[chain]
    }

    override fun addUpstream(chain: Chain, up: Upstream): ChainUpstreams {
        val current = chainMapping[chain]
        if (current == null) {
            val created = ChainUpstreams(chain, ArrayList<Upstream>(), targetFor(chain))
            created.addUpstream(up)
            created.start()
            chainMapping[chain] = created
            chainsBus.onNext(chain)
            return created
        } else {
            current.addUpstream(up)
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

    override fun observeChains(): Flux<Chain> {
        return Flux.merge(
                Flux.fromIterable(getAvailable()),
                Flux.from(chainsBus)
        )
    }

    override fun targetFor(chain: Chain): CallMethods {
        var current = callTargets[chain]
        if (current == null) {
            current = QuorumBasedMethods(objectMapper, chain)
            callTargets[chain] = current
        }
        return current
    }

    override fun isAvailable(chain: Chain): Boolean {
        return chainMapping.containsKey(chain) && callTargets.containsKey(chain)
    }
}