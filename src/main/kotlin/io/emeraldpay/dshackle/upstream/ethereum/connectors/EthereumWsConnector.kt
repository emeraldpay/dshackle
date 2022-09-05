package io.emeraldpay.dshackle.upstream.ethereum.connectors

import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.BlockValidator
import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.ethereum.*
import io.emeraldpay.dshackle.upstream.forkchoice.ForkChoice
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcWsClient
import io.emeraldpay.grpc.Chain

class EthereumWsConnector(
    wsFactory: EthereumWsFactory,
    upstream: DefaultUpstream,
    validator: EthereumUpstreamValidator,
    chain: Chain,
    forkChoice: ForkChoice,
    blockValidator: BlockValidator
) : EthereumConnector {
    private val conn: WsConnection
    private val api: Reader<JsonRpcRequest, JsonRpcResponse>
    private val head: EthereumWsHead

    init {
        conn = wsFactory.create(upstream, validator)
        head = EthereumWsHead(conn, forkChoice, blockValidator)
        api = JsonRpcWsClient(conn)
    }

    override fun start() {
        conn.connect()
        head.start()
    }

    override fun isRunning(): Boolean {
        return head.isRunning
    }

    override fun stop() {
        conn.close()
        head.stop()
    }

    override fun getApi(): Reader<JsonRpcRequest, JsonRpcResponse> {
        return api
    }

    override fun getHead(): Head {
        return head
    }
}
