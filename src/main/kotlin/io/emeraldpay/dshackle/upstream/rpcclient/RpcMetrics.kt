/**
 * Copyright (c) 2021 EmeraldPay, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.emeraldpay.dshackle.upstream.rpcclient

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Timer
import reactor.core.publisher.Mono
import reactor.netty.ChannelPipelineConfigurer
import reactor.netty.channel.ChannelMetricsRecorder
import reactor.netty.channel.ChannelOperations
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Function

class RpcMetrics(
    tags: Iterable<Tag>,
    val timer: Timer,
    val fails: Counter,
    val responseSize: DistributionSummary,

    // A standard Metrics Recorder supported by Reactor Netty, so pass it to the connections responsible for RPC operations
    val connectionMetrics: ChannelMetricsRecorder,
) {

    private val connectionTime = Timer.builder("netty.client.connection_time")
        .tags(tags)
        .register(Metrics.globalRegistry)

    private val queueSize = AtomicInteger(0)

    val onChannelInit: ChannelPipelineConfigurer
        get() = ChannelPipelineConfigurer { connectionObserver, channel, remoteAddress ->
            // See reactor.netty.transport.TransportConfig$TransportChannelInitializer
            // By default it creates a bunch of other metrics to monitor memory allocation, connection pool, connection time, etc.,
            // which are not very applicable to the Dshackle usage scenarios.
            // But we only register a basic ChannelMetricsRecorder with metrics and tags specific to Dshackle
            ChannelOperations.addMetricsHandler(channel, connectionMetrics, remoteAddress, false)
        }

    val processResponseSize: Function<Mono<JsonRpcResponse>, Mono<JsonRpcResponse>>
        get() = java.util.function.Function {
            it.doOnNext { response ->
                if (response.hasResult()) {
                    responseSize.record(response.resultOrEmpty.size.toDouble())
                }
            }
        }

    init {
        Gauge.builder("upstream.rpc.queue_size", queueSize.get()::toDouble)
            .tags(tags)
            .register(Metrics.globalRegistry)
    }

    /**
     * Record the total connection time.
     * The difference from #recordConnectTime is that this one is applicable to long
     */
    fun recordConnectionTime(time: Duration) {
        connectionTime.record(time)
    }

    /**
     * Call when a new request is added to the queue, i.e., is about to be sent to the upstream
     */
    fun onMessageEnqueued() {
        queueSize.incrementAndGet()
    }

    /**
     * Call when a request it processed, as success, error, timeout or any other status indicating that the request is no longer in queue.
     */
    fun onMessageFinished() {
        queueSize.decrementAndGet()
    }
}
