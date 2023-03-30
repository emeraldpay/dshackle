package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.upstream.ethereum.WsConnection
import io.emeraldpay.dshackle.upstream.ethereum.WsSubscriptions
import reactor.core.publisher.Flux

class WebsocketConnectionStatesHandler(
    private val wsSubscriptions: WsSubscriptions,
    private val onConnected: () -> Unit
) {
    private var connectionId: String? = null
    private var tryResubscribe = false

    init {
        wsSubscriptions.connectionInfoFlux()
            .subscribe {
                if (it.connectionId == connectionId && it.connectionState == WsConnection.ConnectionState.DISCONNECTED) {
                    connectionId = null
                    if (tryResubscribe) {
                        tryResubscribe = false
                    }
                } else if (connectionId == null && it.connectionState == WsConnection.ConnectionState.CONNECTED) {
                    if (!tryResubscribe) {
                        tryResubscribe = true
                        onConnected()
                    }
                }
            }
    }

    fun subscribe(method: String): Flux<ByteArray> =
        wsSubscriptions.subscribe(method)
            .also {
                connectionId = it.connectionId
                tryResubscribe = false
            }.data
}
