/**
 * Copyright (c) 2020 EmeraldPay, Inc
 * Copyright (c) 2019 ETCDEV GmbH
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
package io.emeraldpay.dshackle.rpc

import brave.Tracer
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.api.proto.ReactorBlockchainGrpc
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.ChainValue
import io.emeraldpay.dshackle.config.spans.ProviderSpanHandler
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Timer
import org.apache.commons.lang3.RandomStringUtils
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.DependsOn
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

@Service
@DependsOn("monitoringSetup")
class BlockchainRpc(
    private val nativeCall: NativeCall,
    private val nativeSubscribe: NativeSubscribe,
    private val streamHead: StreamHead,
    private val describe: Describe,
    private val subscribeStatus: SubscribeStatus,
    private val subscribeNodeStatus: SubscribeNodeStatus,
    @Qualifier("rpcScheduler")
    private val scheduler: Scheduler,
    @Autowired(required = false)
    private val providerSpanHandler: ProviderSpanHandler?,
    private val tracer: Tracer,
) : ReactorBlockchainGrpc.BlockchainImplBase() {

    private val log = LoggerFactory.getLogger(BlockchainRpc::class.java)

    private val describeMetric = Counter.builder("request.grpc.request")
        .tag("type", "describe")
        .tag("chain", "NA")
        .register(Metrics.globalRegistry)
    private val subscribeStatusMetric = Counter.builder("request.grpc.request")
        .tag("type", "subscribeStatus")
        .tag("chain", "NA")
        .register(Metrics.globalRegistry)
    private val failMetric = Counter.builder("request.grpc.fail")
        .description("Number of requests failed to process")
        .register(Metrics.globalRegistry)
    private val chainMetrics = ChainValue { chain -> RequestMetrics(chain) }

    override fun nativeCall(request: Mono<BlockchainOuterClass.NativeCallRequest>): Flux<BlockchainOuterClass.NativeCallReplyItem> {
        var startTime = 0L
        var metrics: RequestMetrics? = null
        val idsMap = mutableMapOf<Int, String>()
        return nativeCall.nativeCall(
            request
                .subscribeOn(scheduler)
                .doOnNext { req ->
                    metrics = chainMetrics.get(req.chain)
                    metrics?.let { m ->
                        m.nativeCallMetric.increment()
                        req.itemsList.forEach { item ->
                            idsMap[item.id] = item.method
                            m.getNativeItemMetrics(item.method).nativeItemRequest.increment()
                        }
                    }
                    startTime = System.currentTimeMillis()
                },
        ).doOnNext { reply ->
            metrics?.getNativeItemMetrics(idsMap[reply.id] ?: "unknown")?.let { itemMetrics ->
                itemMetrics.nativeItemResponse.record(
                    System.currentTimeMillis() - startTime,
                    TimeUnit.MILLISECONDS,
                )
                if (!reply.succeed) {
                    itemMetrics.nativeItemResponseErr.increment()
                }
            }
        }.doOnError {
            failMetric.increment()
        }.doFinally {
            tracer.currentSpan()?.run {
                providerSpanHandler?.sendSpans(this.context())
            }
        }
    }

    override fun nativeSubscribe(request: Mono<BlockchainOuterClass.NativeSubscribeRequest>): Flux<BlockchainOuterClass.NativeSubscribeReplyItem> {
        var metrics: RequestMetrics? = null
        return nativeSubscribe.nativeSubscribe(
            request
                .doOnNext {
                    metrics = chainMetrics.get(it.chain)
                    metrics!!.nativeSubscribeMetric.increment()
                },
        ).doOnNext {
            metrics?.nativeSubscribeRespMetric?.increment()
        }.doOnError { failMetric.increment() }
    }

    override fun subscribeHead(request: Mono<Common.Chain>): Flux<BlockchainOuterClass.ChainHead> {
        return streamHead.add(
            request
                .doOnNext { chainMetrics.get(it.type).subscribeHeadMetric.increment() },
        ).doOnError { failMetric.increment() }
    }

    override fun describe(request: Mono<BlockchainOuterClass.DescribeRequest>): Mono<BlockchainOuterClass.DescribeResponse> {
        describeMetric.increment()
        return describe.describe(request)
            .doOnError { failMetric.increment() }
    }

    override fun subscribeStatus(request: Mono<BlockchainOuterClass.StatusRequest>): Flux<BlockchainOuterClass.ChainStatus> {
        subscribeStatusMetric.increment()
        return subscribeStatus.subscribeStatus(request)
            .doOnError { failMetric.increment() }
    }

    override fun subscribeNodeStatus(request: Mono<BlockchainOuterClass.SubscribeNodeStatusRequest>): Flux<BlockchainOuterClass.NodeStatusResponse> {
        return request.flatMapMany {
            val subId = it.traceId.takeIf { it.isNotBlank() } ?: RandomStringUtils.randomAlphanumeric(8)
            subscribeNodeStatus.subscribe(it).subscribeOn(scheduler)
                .doOnError { err ->
                    log.error("Error during processing node subscription [$subId], closing", err)
                    failMetric.increment()
                }
                .doFinally { sig ->
                    log.info("Closing node status subscription named [$subId] with $sig")
                }
        }
    }

    class RequestMetrics(val chain: Chain) {
        val nativeCallMetric = Counter.builder("request.grpc.request")
            .tag("type", "nativeCall")
            .tag("chain", chain.chainCode)
            .register(Metrics.globalRegistry)
        val nativeSubscribeMetric = Counter.builder("request.grpc.request")
            .tag("type", "nativeSubscribe")
            .tag("chain", chain.chainCode)
            .register(Metrics.globalRegistry)
        val nativeSubscribeRespMetric = Counter.builder("request.grpc.response")
            .tag("type", "nativeSubscribe")
            .tag("chain", chain.chainCode)
            .register(Metrics.globalRegistry)
        val subscribeHeadMetric = Counter.builder("request.grpc.request")
            .tag("type", "subscribeHead")
            .tag("chain", chain.chainCode)
            .register(Metrics.globalRegistry)
        val subscribeHeadRespMetric = Counter.builder("request.grpc.reply")
            .tag("type", "subscribeHead")
            .tag("chain", chain.chainCode)
            .register(Metrics.globalRegistry)
        val subscribeTxMetric = Counter.builder("request.grpc.request")
            .tag("type", "subscribeTx")
            .tag("chain", chain.chainCode)
            .register(Metrics.globalRegistry)
        val subscribeBalanceMetric = Counter.builder("request.grpc.request")
            .tag("type", "subscribeBalance")
            .tag("chain", chain.chainCode)
            .register(Metrics.globalRegistry)
        val subscribeBalanceRespMetric = Counter.builder("request.grpc.reply")
            .tag("type", "subscribeBalance")
            .tag("chain", chain.chainCode)
            .register(Metrics.globalRegistry)
        val getBalanceMetric = Counter.builder("request.grpc.request")
            .tag("type", "getBalance")
            .tag("chain", chain.chainCode)
            .register(Metrics.globalRegistry)
        val getBalanceRespMetric = Timer.builder("request.grpc.response")
            .tag("type", "getBalance")
            .tag("chain", chain.chainCode)
            .publishPercentileHistogram()
            .register(Metrics.globalRegistry)
        val estimateFeeMetric = Counter.builder("request.grpc.request")
            .tag("type", "estimateFee")
            .tag("chain", chain.chainCode)
            .register(Metrics.globalRegistry)
        val estimateFeeRespMetric = Timer.builder("request.grpc.response")
            .tag("type", "estimateFee")
            .tag("chain", chain.chainCode)
            .publishPercentileHistogram()
            .register(Metrics.globalRegistry)

        private val nativeItemMetrics = ConcurrentHashMap<String, NativeRequestItemsMetrics>()

        fun getNativeItemMetrics(method: String) =
            nativeItemMetrics.computeIfAbsent(method) { NativeRequestItemsMetrics(chain, it) }
    }

    class NativeRequestItemsMetrics(chain: Chain, method: String) {
        val nativeItemRequest = Counter.builder("request.grpc.native.request")
            .tag("chain", chain.chainCode)
            .tag("method", method)
            .register(Metrics.globalRegistry)
        val nativeItemResponse = Timer.builder("request.grpc.native.response")
            .tag("chain", chain.chainCode)
            .tag("method", method)
            .publishPercentileHistogram()
            .register(Metrics.globalRegistry)
        val nativeItemResponseErr = Counter.builder("request.grpc.native.request.err")
            .tag("chain", chain.chainCode)
            .tag("method", method)
            .register(Metrics.globalRegistry)
    }
}
