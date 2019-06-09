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
    private val chainMapping = HashMap<Chain, List<EthereumUpstream>>()

    @PostConstruct
    fun start() {
        env.getProperty("upstream.ethereum")?.let {
            chainMapping[Chain.ETHEREUM] = listOf(buildClient(it, Chain.ETHEREUM))
        }
        env.getProperty("upstream.ethereumclassic")?.let {
            chainMapping[Chain.ETHEREUM_CLASSIC] = listOf(buildClient(it, Chain.ETHEREUM_CLASSIC))
        }
        env.getProperty("upstream.ethereumclassic.ws")?.let {
            chainMapping[Chain.ETHEREUM_CLASSIC]!![0].ws = buildWs(it, Chain.ETHEREUM_CLASSIC)
        }
        env.getProperty("upstream.morden")?.let {
            chainMapping[Chain.MORDEN] = listOf(buildClient(it, Chain.MORDEN))
        }
    }

    private fun buildClient(url: String, chain: Chain): EthereumUpstream {
        return EthereumUpstream(
                DefaultRpcClient(DefaultRpcTransport(URI(url))),
                objectMapper,
                chain
        )
    }

    private fun buildWs(url: String, chain: Chain): EthereumWsUpstream {
        val ws = EthereumWsUpstream(
                URI(url),
                URI("http://localhost")
        )
        ws.connect()
        return ws
    }

    fun validateUpstream(upstream: EthereumUpstream): Boolean {
        return true
    }

    fun ethereumUpstream(chain: Chain): EthereumUpstream? {
        val all = chainMapping[chain] ?: return null
        return all[seq++ % all.size]
    }
}