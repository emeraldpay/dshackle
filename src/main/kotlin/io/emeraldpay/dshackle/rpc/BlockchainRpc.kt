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

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.api.proto.ReactorBlockchainGrpc
import io.emeraldpay.dshackle.ChainValue
import io.emeraldpay.dshackle.SilentException
import io.emeraldpay.grpc.Chain
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Timer
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

@Service
class BlockchainRpc(
        @Autowired private val nativeCall: NativeCall,
        @Autowired private val streamHead: StreamHead,
        @Autowired private val trackTx: List<TrackTx>,
        @Autowired private val trackAddress: List<TrackAddress>,
        @Autowired private val describe: Describe,
        @Autowired private val subscribeStatus: SubscribeStatus
): ReactorBlockchainGrpc.BlockchainImplBase() {

    private val log = LoggerFactory.getLogger(BlockchainRpc::class.java)

    private val describeMetric = Counter.builder("request.grpc.request")
            .tag("type", "describe")
            .register(Metrics.globalRegistry)
    private val subscribeStatusMetric = Counter.builder("request.grpc.request")
            .tag("type", "subscribeStatus")
            .register(Metrics.globalRegistry)
    private val errorMetric = Counter.builder("request.grpc.err")
            .register(Metrics.globalRegistry)
    private val chainMetrics = ChainValue { chain -> RequestMetrics(chain) }

    override fun nativeCall(request: Mono<BlockchainOuterClass.NativeCallRequest>): Flux<BlockchainOuterClass.NativeCallReplyItem> {
        var startTime = 0L
        var metrics: RequestMetrics? = null
        return nativeCall.nativeCall(
                request
                        .doOnNext {
                            metrics = chainMetrics.get(it.chain)
                            metrics!!.nativeCallMetric.increment()
                            startTime = System.currentTimeMillis()
                        }
        ).doOnNext {
            metrics?.nativeCallRespMetric?.record(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS)
        }.doOnError { errorMetric.increment() }
    }

    override fun subscribeHead(request: Mono<Common.Chain>): Flux<BlockchainOuterClass.ChainHead> {
        return streamHead.add(
                request
                        .doOnNext { chainMetrics.get(it.type).subscribeHeadMetric.increment() }
        ).doOnError { errorMetric.increment() }
    }

    override fun subscribeTxStatus(request: Mono<BlockchainOuterClass.TxStatusRequest>): Flux<BlockchainOuterClass.TxStatus> {
        return request.flatMapMany { request ->
            val chain = Chain.byId(request.chainValue)
            val metrics = chainMetrics.get(chain)
            metrics.subscribeTxMetric.increment()
            try {
                trackTx.find { it.isSupported(chain) }?.let { track ->
                    track.subscribe(request)
                            .doOnNext { metrics.subscribeHeadRespMetric.increment() }
                            .doOnError { errorMetric.increment() }
                } ?: Flux.error(SilentException.UnsupportedBlockchain(chain))
            } catch (t: Throwable) {
                log.error("Internal error during Tx Subscription", t)
                errorMetric.increment()
                Flux.error<BlockchainOuterClass.TxStatus>(IllegalStateException("Internal Error"))
            }
        }
    }

    override fun subscribeBalance(requestMono: Mono<BlockchainOuterClass.BalanceRequest>): Flux<BlockchainOuterClass.AddressBalance> {
        return requestMono.flatMapMany { request ->
            val chain = Chain.byId(request.asset.chainValue)
            val metrics = chainMetrics.get(chain)
            metrics.subscribeBalanceMetric.increment()
            val asset = request.asset.code.toLowerCase()
            try {
                trackAddress.find { it.isSupported(chain, asset) }?.let { track ->
                    track.subscribe(request)
                            .doOnNext { metrics.subscribeBalanceRespMetric.increment() }
                            .doOnError { errorMetric.increment() }
                } ?: Flux.error<BlockchainOuterClass.AddressBalance>(SilentException.UnsupportedBlockchain(chain))
                                .doOnSubscribe {
                                    log.error("Balance for $chain:$asset is not supported")
                                }
            } catch (t: Throwable) {
                log.error("Internal error during Balance Subscription", t)
                errorMetric.increment()
                Flux.error<BlockchainOuterClass.AddressBalance>(IllegalStateException("Internal Error"))
            }
        }
    }

    override fun getBalance(requestMono: Mono<BlockchainOuterClass.BalanceRequest>): Flux<BlockchainOuterClass.AddressBalance> {
        return requestMono.flatMapMany { request ->
            val chain = Chain.byId(request.asset.chainValue)
            val metrics = chainMetrics.get(chain)
            metrics.getBalanceMetric.increment()
            val asset = request.asset.code.toLowerCase()
            val startTime = System.currentTimeMillis()
            try {
                trackAddress.find { it.isSupported(chain, asset) }?.let { track ->
                    track.getBalance(request)
                            .doOnNext {
                                metrics.getBalanceRespMetric.record(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS)
                            }
                } ?: Flux.error<BlockchainOuterClass.AddressBalance>(SilentException.UnsupportedBlockchain(chain))
                                .doOnSubscribe {
                                    log.error("Balance for $chain:$asset is not supported")
                                }
            } catch (t: Throwable) {
                log.error("Internal error during Balance Request", t)
                errorMetric.increment()
                Flux.error<BlockchainOuterClass.AddressBalance>(IllegalStateException("Internal Error"))
            }
        }
    }

    override fun describe(request: Mono<BlockchainOuterClass.DescribeRequest>): Mono<BlockchainOuterClass.DescribeResponse> {
        describeMetric.increment()
        return describe.describe(request)
                .doOnError { errorMetric.increment() }
    }

    override fun subscribeStatus(request: Mono<BlockchainOuterClass.StatusRequest>): Flux<BlockchainOuterClass.ChainStatus> {
        subscribeStatusMetric.increment()
        return subscribeStatus.subscribeStatus(request)
                .doOnError { errorMetric.increment() }
    }

    class RequestMetrics(chain: Chain) {
        val nativeCallMetric = Counter.builder("request.grpc.request")
                .tag("type", "nativeCall")
                .tag("chain", chain.chainCode)
                .register(Metrics.globalRegistry)
        val nativeCallRespMetric = Timer.builder("request.grpc.response")
                .tag("type", "nativeCall")
                .tag("chain", chain.chainCode)
                .publishPercentileHistogram()
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
    }
}