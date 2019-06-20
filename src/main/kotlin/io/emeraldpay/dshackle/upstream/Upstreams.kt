package io.emeraldpay.dshackle.upstream

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.config.UpstreamsReader
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.rpc.DefaultRpcClient
import io.infinitape.etherjar.rpc.transport.DefaultRpcTransport
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.env.Environment
import org.springframework.stereotype.Repository
import java.io.File
import java.net.URI
import javax.annotation.PostConstruct

@Repository
class Upstreams(
        @Autowired val env: Environment,
        @Autowired private val objectMapper: ObjectMapper
) {

    private val log = LoggerFactory.getLogger(Upstreams::class.java)
    private var seq = 0
    private val chainMapping = HashMap<Chain, ArrayList<Upstream>>()

    private val chainNames = mapOf(
            "ethereum" to Chain.ETHEREUM,
            "ethereum-classic" to Chain.ETHEREUM_CLASSIC,
            "morden" to Chain.MORDEN
    )

    @PostConstruct
    fun start() {
        val path = env.getProperty("upstreams.config")
        if (StringUtils.isEmpty(path)) {
            log.error("Path to upstreams is not set (upstreams.config)")
            System.exit(1)
        }
        val upstreamConfig = File(path)
        val ok = upstreamConfig.exists() && upstreamConfig.isFile
        if (!ok) {
            log.error("Unable to setup upstreams from ${upstreamConfig.path}")
            System.exit(1)
        }
        log.info("Read upstream configuration from ${upstreamConfig.path}")
        val reader = UpstreamsReader()
        val config = reader.read(upstreamConfig.inputStream())

        config.upstreams.forEach { up ->
            val chain = chainNames[up.chain] ?: return@forEach
            var rpcApi: EthereumApi? = null
            var wsApi: EthereumWs? = null
            val urls = ArrayList<URI>()
            up.endpoints.forEach { endpoint ->
                if (endpoint.type == io.emeraldpay.dshackle.config.Upstreams.EndpointType.JSON_RPC) {
                    rpcApi = EthereumApi(
                            DefaultRpcClient(DefaultRpcTransport(endpoint.url)),
                            objectMapper,
                            chain
                    )
                }
                if (endpoint.type == io.emeraldpay.dshackle.config.Upstreams.EndpointType.WEBSOCKET) {
                    wsApi = EthereumWs(
                            endpoint.url,
                            endpoint.origin ?: URI("http://localhost")
                    )
                }
                urls.add(endpoint.url)
            }
            if (rpcApi != null) {
                log.info("Info using ${chain.chainName} upstream, at ${urls.joinToString()}")
                val current = chainMapping[chain] ?: ArrayList()
                current.add(Upstream(chain, rpcApi!!, wsApi))
                chainMapping[chain] = current
            }
        }
    }

    fun ethereumUpstream(chain: Chain): Upstream? {
        val list = chainMapping[chain]
        if (list == null || list.isEmpty()) {
            return null
        }
        val i = seq++
        if (seq >= Int.MAX_VALUE / 2) {
            seq = 0
        }
        return list.get(i % list.size)
    }
}