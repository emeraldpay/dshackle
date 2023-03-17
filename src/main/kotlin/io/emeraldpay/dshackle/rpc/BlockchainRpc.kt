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

import com.google.protobuf.util.JsonFormat
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.api.proto.ReactorBlockchainGrpc
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.ChainValue
import io.emeraldpay.dshackle.SilentException
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Timer
import org.apache.commons.lang3.RandomStringUtils
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.DependsOn
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler
import java.util.Locale
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

@Service
@DependsOn("monitoringSetup")
class BlockchainRpc(
    private val nativeCall: NativeCall,
    private val nativeSubscribe: NativeSubscribe,
    private val streamHead: StreamHead,
    private val trackTx: List<TrackTx>,
    private val trackAddress: List<TrackAddress>,
    private val describe: Describe,
    private val subscribeStatus: SubscribeStatus,
    private val estimateFee: EstimateFee,
    private val subscribeNodeStatus: SubscribeNodeStatus,
    @Qualifier("rpcScheduler")
    private val scheduler: Scheduler
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
                }
        ).doOnNext { reply ->
            metrics?.getNativeItemMetrics(idsMap[reply.id] ?: "unknown")?.let { itemMetrics ->
                itemMetrics.nativeItemResponse.record(
                    System.currentTimeMillis() - startTime,
                    TimeUnit.MILLISECONDS
                )
                if (!reply.succeed) {
                    itemMetrics.nativeItemResponseErr.increment()
                }
            }
        }.doOnError { failMetric.increment() }
    }

    override fun nativeSubscribe(request: Mono<BlockchainOuterClass.NativeSubscribeRequest>): Flux<BlockchainOuterClass.NativeSubscribeReplyItem> {
        var metrics: RequestMetrics? = null
        return nativeSubscribe.nativeSubscribe(
            request
                .doOnNext {
                    metrics = chainMetrics.get(it.chain)
                    metrics!!.nativeSubscribeMetric.increment()
                }
        ).doOnNext {
            metrics?.nativeSubscribeRespMetric?.increment()
        }.doOnError { failMetric.increment() }
    }

    override fun subscribeHead(request: Mono<Common.Chain>): Flux<BlockchainOuterClass.ChainHead> {
        return streamHead.add(
            request
                .doOnNext { chainMetrics.get(it.type).subscribeHeadMetric.increment() }
        ).doOnError { failMetric.increment() }
    }

    override fun subscribeTxStatus(requestMono: Mono<BlockchainOuterClass.TxStatusRequest>): Flux<BlockchainOuterClass.TxStatus> {
        return requestMono.subscribeOn(scheduler).flatMapMany { request ->
            val chain = Chain.byId(request.chainValue)
            val metrics = chainMetrics.get(chain)
            metrics.subscribeTxMetric.increment()
            try {
                trackTx.find { it.isSupported(chain) }?.let { track ->
                    track.subscribe(request)
                        .doOnNext { metrics.subscribeHeadRespMetric.increment() }
                        .doOnError { failMetric.increment() }
                } ?: Flux.error(SilentException.UnsupportedBlockchain(chain))
            } catch (t: Throwable) {
                log.error("Internal error during Tx Subscription", t)
                failMetric.increment()
                Flux.error(IllegalStateException("Internal Error"))
            }
        }
    }

    override fun subscribeBalance(requestMono: Mono<BlockchainOuterClass.BalanceRequest>): Flux<BlockchainOuterClass.AddressBalance> {
        return requestMono.subscribeOn(scheduler).flatMapMany { request ->
            val chain = Chain.byId(request.asset.chainValue)
            val metrics = chainMetrics.get(chain)
            metrics.subscribeBalanceMetric.increment()
            val asset = request.asset.code.lowercase(Locale.getDefault())
            try {
                trackAddress.find { it.isSupported(chain, asset) }?.let { track ->
                    track.subscribe(request)
                        .doOnNext { metrics.subscribeBalanceRespMetric.increment() }
                        .doOnError { failMetric.increment() }
                } ?: Flux.error<BlockchainOuterClass.AddressBalance>(SilentException.UnsupportedBlockchain(chain))
                    .doOnSubscribe {
                        log.error("Balance for $chain:$asset is not supported")
                    }
            } catch (t: Throwable) {
                log.error("Internal error during Balance Subscription", t)
                failMetric.increment()
                Flux.error(IllegalStateException("Internal Error"))
            }
        }
    }

    override fun getBalance(requestMono: Mono<BlockchainOuterClass.BalanceRequest>): Flux<BlockchainOuterClass.AddressBalance> {
        return requestMono.subscribeOn(scheduler).flatMapMany { request ->
            val chain = Chain.byId(request.asset.chainValue)
            val metrics = chainMetrics.get(chain)
            metrics.getBalanceMetric.increment()
            val asset = request.asset.code.lowercase(Locale.getDefault())
            val startTime = System.currentTimeMillis()
            try {
                trackAddress.find { it.isSupported(chain, asset) }?.let { track ->
                    track.getBalance(request)
                        .doOnNext {
                            metrics.getBalanceRespMetric.record(
                                System.currentTimeMillis() - startTime,
                                TimeUnit.MILLISECONDS
                            )
                        }
                } ?: Flux.error<BlockchainOuterClass.AddressBalance>(SilentException.UnsupportedBlockchain(chain))
                    .doOnSubscribe {
                        log.error("Balance for $chain:$asset is not supported")
                    }
            } catch (t: Throwable) {
                log.error("Internal error during Balance Request", t)
                failMetric.increment()
                Flux.error<BlockchainOuterClass.AddressBalance>(IllegalStateException("Internal Error"))
            }
        }
    }

    override fun estimateFee(request: Mono<BlockchainOuterClass.EstimateFeeRequest>): Mono<BlockchainOuterClass.EstimateFeeResponse> {
        return request
            .subscribeOn(scheduler)
            .flatMap {
                val chain = Chain.byId(it.chainValue)
                val metrics = chainMetrics.get(chain)
                metrics.estimateFeeMetric.increment()
                val startTime = System.currentTimeMillis()
                estimateFee.estimateFee(it).doFinally {
                    metrics.estimateFeeRespMetric.record(
                        System.currentTimeMillis() - startTime,
                        TimeUnit.MILLISECONDS
                    )
                }
            }
            .doOnError { t ->
                log.error("Internal error during Fee Estimation", t)
                failMetric.increment()
            }
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
                    log.info("Closing node status subscription named $subId with $sig")
                }
                .doOnNext { elem ->
                    log.debug(
                        "Emitted next node status to [$subId] with data [${
                        JsonFormat.printer().omittingInsignificantWhitespace().print(elem)
                        }]"
                    )
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
