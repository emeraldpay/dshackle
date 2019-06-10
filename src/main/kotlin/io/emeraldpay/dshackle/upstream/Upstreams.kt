package io.emeraldpay.dshackle.upstream

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.rpc.DefaultRpcClient
import io.infinitape.etherjar.rpc.transport.DefaultRpcTransport
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.env.Environment
import org.springframework.stereotype.Repository
import java.net.URI
import javax.annotation.PostConstruct

@Repository
class Upstreams(
        @Autowired val env: Environment,
        @Autowired private val objectMapper: ObjectMapper
) {

    private var seq = 0
    private val chainMapping = HashMap<Chain, Upstream>()

    @PostConstruct
    fun start() {
        env.getProperty("upstream.ethereum")?.let {
            val api = buildClient(it, Chain.ETHEREUM)
            chainMapping[Chain.ETHEREUM] = Upstream(Chain.ETHEREUM, api)
        }
        env.getProperty("upstream.ethereumclassic")?.let {
            val api = buildClient(it, Chain.ETHEREUM_CLASSIC)
            val ws = if (env.containsProperty("upstream.ethereumclassic.ws")) {
                buildWs(env.getProperty("upstream.ethereumclassic.ws")!!, Chain.ETHEREUM_CLASSIC)
            } else {
                null
            }
            chainMapping[Chain.ETHEREUM_CLASSIC] = Upstream(Chain.ETHEREUM_CLASSIC, api, ws)
        }
        env.getProperty("upstream.morden")?.let {
            val api = buildClient(it, Chain.MORDEN)
            chainMapping[Chain.MORDEN] = Upstream(Chain.MORDEN, api)
        }
    }

    private fun buildClient(url: String, chain: Chain): EthereumApi {
        return EthereumApi(
                DefaultRpcClient(DefaultRpcTransport(URI(url))),
                objectMapper,
                chain
        )
    }

    private fun buildWs(url: String, chain: Chain): EthereumWs {
        val ws = EthereumWs(
                URI(url),
                URI("http://localhost")
        )
        ws.connect()
        return ws
    }

    fun ethereumUpstream(chain: Chain): Upstream? {
        return chainMapping[chain]
    }
}