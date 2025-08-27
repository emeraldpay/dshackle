package io.emeraldpay.dshackle.test

import reactor.netty.channel.ChannelMetricsRecorder
import java.net.SocketAddress
import java.time.Duration

class NoOpChannelMetricsRecorder : ChannelMetricsRecorder {
    companion object {
        val Instance = NoOpChannelMetricsRecorder()
    }

    override fun recordDataReceived(
        remoteAddress: SocketAddress,
        bytes: Long,
    ) {
    }

    override fun recordDataSent(
        remoteAddress: SocketAddress,
        bytes: Long,
    ) {
    }

    override fun incrementErrorsCount(remoteAddress: SocketAddress) {
    }

    override fun recordTlsHandshakeTime(
        remoteAddress: SocketAddress,
        time: Duration,
        status: String,
    ) {
    }

    override fun recordConnectTime(
        remoteAddress: SocketAddress,
        time: Duration,
        status: String,
    ) {
    }

    override fun recordResolveAddressTime(
        remoteAddress: SocketAddress,
        time: Duration,
        status: String,
    ) {
    }
}
