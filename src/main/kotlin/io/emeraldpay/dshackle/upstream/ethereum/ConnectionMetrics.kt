package io.emeraldpay.dshackle.upstream.ethereum

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Tag
import reactor.netty.channel.ChannelMetricsRecorder
import java.net.SocketAddress
import java.time.Duration

/**
 * Connection Metrics specific to Dshackle. The main difference to standard metrics provided with Reactor Netty is that
 * (a) we don't record the times, b/c we have long term connections via WS or gRPC, so it make no sense to measure them,
 * (b) no protocol part included into the name, and no `reactor` prefix
 * (c) includes upstream id tag, instead of a remote IP tag
 *
 * @see reactor.netty.http.MicrometerHttpMetricsRecorder
 */
class ConnectionMetrics(
    tags: Iterable<Tag>,
) : ChannelMetricsRecorder {
    private val dataReceived =
        DistributionSummary
            .builder("netty.client.data.received")
            .tags(tags)
            .register(Metrics.globalRegistry)
    private val dataSent =
        DistributionSummary
            .builder("netty.client.data.sent")
            .tags(tags)
            .register(Metrics.globalRegistry)
    private val errors =
        Counter
            .builder("netty.client.errors")
            .tags(tags)
            .register(Metrics.globalRegistry)

    override fun recordDataReceived(
        remoteAddress: SocketAddress,
        bytes: Long,
    ) {
        dataReceived.record(bytes.toDouble())
    }

    override fun recordDataSent(
        remoteAddress: SocketAddress,
        bytes: Long,
    ) {
        dataSent.record(bytes.toDouble())
    }

    override fun incrementErrorsCount(remoteAddress: SocketAddress) {
        errors.increment()
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
