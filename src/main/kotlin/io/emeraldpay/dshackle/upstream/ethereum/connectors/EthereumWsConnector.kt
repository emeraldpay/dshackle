package io.emeraldpay.dshackle.upstream.ethereum.connectors

import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.BlockValidator
import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.ethereum.*
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.EthereumWsIngressSubscription
import io.emeraldpay.dshackle.upstream.forkchoice.ForkChoice
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcWsClient

class EthereumWsConnector(
    wsFactory: EthereumWsFactory,
    upstream: DefaultUpstream,
    forkChoice: ForkChoice,
    blockValidator: BlockValidator
) : EthereumConnector {
    private val conn: WsConnectionImpl
    private val api: Reader<JsonRpcRequest, JsonRpcResponse>
    private val head: EthereumWsHead
    private val subscriptions: EthereumIngressSubscription

    init {
        conn = wsFactory.create(upstream)
        api = JsonRpcWsClient(conn)
        val wsSubscriptions = WsSubscriptionsImpl(conn)
        head = EthereumWsHead(upstream.getId(), forkChoice, blockValidator, api, wsSubscriptions)
        subscriptions = EthereumWsIngressSubscription(wsSubscriptions)
    }

    override fun start() {
        conn.connect()
        head.start()
    }

    override fun isRunning(): Boolean {
        return head.isRunning()
    }

    override fun stop() {
        conn.close()
        head.stop()
    }

    override fun getApi(): Reader<JsonRpcRequest, JsonRpcResponse> {
        return api
    }

    override fun getIngressSubscription(): EthereumIngressSubscription {
        return subscriptions
    }

    override fun getHead(): Head {
        return head
    }
}
