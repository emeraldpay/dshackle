package io.emeraldpay.dshackle.upstream.ethereum.connectors

import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.cache.CachesEnabled
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.BlockValidator
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.MergedHead
import io.emeraldpay.dshackle.upstream.ethereum.EthereumRpcHead
import io.emeraldpay.dshackle.upstream.ethereum.EthereumWsFactory
import io.emeraldpay.dshackle.upstream.ethereum.EthereumWsHead
import io.emeraldpay.dshackle.upstream.ethereum.WsConnection
import io.emeraldpay.dshackle.upstream.forkchoice.ForkChoice
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import java.time.Duration

class EthereumRpcConnector(
    private val directReader: Reader<JsonRpcRequest, JsonRpcResponse>,
    wsFactory: EthereumWsFactory?,
    id: String,
    forkChoice: ForkChoice,
    blockValidator: BlockValidator
) : EthereumConnector, CachesEnabled {
    private val conn: WsConnection?
    private val head: Head

    companion object {
        private val log = LoggerFactory.getLogger(EthereumRpcConnector::class.java)
    }

    init {
        if (wsFactory != null) {
            // do not set upstream to the WS, since it doesn't control the RPC upstream
            conn = wsFactory.create(id, null, null)
            val wsHead = EthereumWsHead(conn, id, forkChoice, blockValidator)
            // receive bew blocks through WebSockets, but also periodically verify with RPC in case if WS failed
            val rpcHead = EthereumRpcHead(directReader, forkChoice, id, blockValidator, Duration.ofSeconds(60))
            head = MergedHead(listOf(rpcHead, wsHead), forkChoice)
        } else {
            conn = null
            log.warn("Setting up connector for $id upstream with RPC-only access, less effective than WS+RPC")
            head = EthereumRpcHead(directReader, forkChoice, id, blockValidator)
        }
    }

    override fun setCaches(caches: Caches) {
        if (head is CachesEnabled) {
            head.setCaches(caches)
        }
    }

    override fun start() {
        if (head is Lifecycle) {
            head.start()
        }
        conn?.connect()
    }

    override fun isRunning(): Boolean {
        if (head is Lifecycle) {
            return head.isRunning
        }
        return true
    }

    override fun stop() {
        if (head is Lifecycle) {
            head.stop()
        }
        conn?.close()
    }

    override fun getApi(): Reader<JsonRpcRequest, JsonRpcResponse> {
        return directReader
    }

    override fun getHead(): Head {
        return head
    }
}
