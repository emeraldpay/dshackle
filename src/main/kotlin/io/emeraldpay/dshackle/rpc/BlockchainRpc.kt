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

import io.emeraldpay.api.Chain
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.api.proto.ReactorBlockchainGrpc
import io.emeraldpay.dshackle.ChainValue
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.SilentException
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Timer
import org.apache.commons.lang3.time.StopWatch
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.DependsOn
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.util.concurrent.TimeUnit

@Service
@DependsOn("monitoringSetup")
class BlockchainRpc(
    @Autowired private val nativeCall: NativeCall,
    @Autowired private val nativeSubscribe: NativeSubscribe,
    @Autowired private val streamHead: StreamHead,
    @Autowired private val trackTx: List<TrackTx>,
    @Autowired private val trackAddress: List<TrackAddress>,
    @Autowired private val trackErc20Allowance: TrackERC20Allowance,
    @Autowired private val describe: Describe,
    @Autowired private val subscribeStatus: SubscribeStatus,
    @Autowired private val estimateFee: EstimateFee,
) : ReactorBlockchainGrpc.BlockchainImplBase() {

    private val log = LoggerFactory.getLogger(BlockchainRpc::class.java)

    private val requestContext = Global.monitoring.egress

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
        return request
            .flatMapMany { req ->
                val timer = StopWatch.createStarted()
                val metrics = chainMetrics.get(req.chain)
                metrics.nativeCallMetric.increment()
                nativeCall.nativeCall(Mono.just(req))
                    .doOnNext { reply ->
                        metrics.nativeCallRespCountMetric.increment()
                        metrics.nativeCallRespTimeMetric.record(timer.nanoTime, TimeUnit.NANOSECONDS)
                        if (!reply.succeed) {
                            metrics.nativeCallErrRespMetric.increment()
                        }
                    }
                    .doOnError { t ->
                        log.error("Unhandled error", t)
                        failMetric.increment()
                    }
                    .contextWrite(requestContext.updateFromGrpc())
            }
            .doOnError { t ->
                log.error("Unhandled error", t)
                failMetric.increment()
            }
    }

    override fun nativeSubscribe(request: Mono<BlockchainOuterClass.NativeSubscribeRequest>): Flux<BlockchainOuterClass.NativeSubscribeReplyItem> {
        return request
            .flatMapMany { req ->
                val metrics = chainMetrics.get(req.chain)
                metrics.nativeSubscribeMetric.increment()
                nativeSubscribe.nativeSubscribe(Mono.just(req))
                    .doOnNext {
                        metrics.nativeSubscribeRespMetric?.increment()
                    }
                    .doOnError { failMetric.increment() }
                    .contextWrite(requestContext.updateFromGrpc())
            }
    }

    override fun subscribeHead(request: Mono<Common.Chain>): Flux<BlockchainOuterClass.ChainHead> {
        return streamHead.add(
            request
                .doOnNext { chainMetrics.get(it.type).subscribeHeadMetric.increment() }
        )
            .doOnError { failMetric.increment() }
            .contextWrite(requestContext.updateFromGrpc())
    }

    override fun subscribeTxStatus(requestMono: Mono<BlockchainOuterClass.TxStatusRequest>): Flux<BlockchainOuterClass.TxStatus> {
        return requestMono.flatMapMany { request ->
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
                Flux.error<BlockchainOuterClass.TxStatus>(IllegalStateException("Internal Error"))
            }
        }
            .contextWrite(requestContext.updateFromGrpc())
    }

    override fun subscribeBalance(requestMono: Mono<BlockchainOuterClass.BalanceRequest>): Flux<BlockchainOuterClass.AddressBalance> {
        return requestMono.flatMapMany { request ->
            val chain = request.getAnyAssetChain()
            val metrics = chainMetrics.get(chain)
            metrics.subscribeBalanceMetric.increment()
            return@flatMapMany try {
                trackAddress.find { it.isSupported(request) }?.let { track ->
                    track.subscribe(request)
                        .doOnNext { metrics.subscribeBalanceRespMetric.increment() }
                        .doOnError { failMetric.increment() }
                } ?: Flux.error<BlockchainOuterClass.AddressBalance>(SilentException.UnsupportedBlockchain(chain))
                    .doOnSubscribe {
                        val asset = request.getAssetCodeOrContractAddress()
                        log.error("Balance for $chain:$asset is not supported")
                    }
            } catch (t: Throwable) {
                log.error("Internal error during Balance Subscription", t)
                failMetric.increment()
                Flux.error<BlockchainOuterClass.AddressBalance>(IllegalStateException("Internal Error"))
            }
        }
            .contextWrite(requestContext.updateFromGrpc())
    }

    override fun getBalance(requestMono: Mono<BlockchainOuterClass.BalanceRequest>): Flux<BlockchainOuterClass.AddressBalance> {
        return requestMono.flatMapMany { request ->
            val timer = StopWatch.createStarted()
            val chain = request.getAnyAssetChain()
            val metrics = chainMetrics.get(chain)
            metrics.getBalanceMetric.increment()
            return@flatMapMany try {
                trackAddress.find { it.isSupported(request) }?.let { track ->
                    track.getBalance(request)
                        .doOnNext {
                            metrics.getBalanceRespCountMetric.increment()
                            metrics.getBalanceRespTimeMetric.record(timer.nanoTime, TimeUnit.NANOSECONDS)
                        }
                } ?: Flux.error<BlockchainOuterClass.AddressBalance>(SilentException.UnsupportedBlockchain(chain))
                    .doOnSubscribe {
                        val asset = request.getAssetCodeOrContractAddress()
                        log.error("Balance for $chain:$asset is not supported")
                    }
            } catch (t: Throwable) {
                log.error("Internal error during Balance Request", t)
                failMetric.increment()
                Flux.error<BlockchainOuterClass.AddressBalance>(IllegalStateException("Internal Error"))
            }
        }
            .contextWrite(requestContext.updateFromGrpc())
    }

    override fun subscribeAddressAllowance(requestMono: Mono<BlockchainOuterClass.AddressAllowanceRequest>): Flux<BlockchainOuterClass.AddressAllowance> {
        return requestMono.flatMapMany { request ->
            val chain = Chain.byId(request.chainValue)
            val metrics = chainMetrics.get(chain)
            metrics.subscribeAddressAllowanceMetric.increment()
            return@flatMapMany try {
                if (trackErc20Allowance.isSupported(request)) {
                    trackErc20Allowance.subscribeAddressAllowance(request)
                        .doOnNext { metrics.subscribeAddressAllowanceRespMetric.increment() }
                        .doOnError { failMetric.increment() }
                } else {
                    Flux.error<BlockchainOuterClass.AddressAllowance>(SilentException.UnsupportedBlockchain(chain))
                        .doOnSubscribe {
                            log.error("Allowance for $chain is not supported")
                        }
                }
            } catch (t: Throwable) {
                log.error("Internal error during Allowance Subscription", t)
                failMetric.increment()
                Flux.error<BlockchainOuterClass.AddressAllowance>(IllegalStateException("Internal Error"))
            }
        }
    }

    override fun getAddressAllowance(requestMono: Mono<BlockchainOuterClass.AddressAllowanceRequest>): Flux<BlockchainOuterClass.AddressAllowance> {
        return requestMono.flatMapMany { request ->
            val timer = StopWatch.createStarted()
            val chain = Chain.byId(request.chainValue)
            val metrics = chainMetrics.get(chain)
            metrics.getAddressAllowanceMetric.increment()
            return@flatMapMany try {
                if (trackErc20Allowance.isSupported(request)) {
                    trackErc20Allowance.getAddressAllowance(request)
                        .doOnNext {
                            metrics.getAddressAllowanceRespCountMetric.increment()
                            metrics.getAddressAllowanceRespTimeMetric.record(timer.nanoTime, TimeUnit.NANOSECONDS)
                        }
                } else {
                    Flux.error<BlockchainOuterClass.AddressAllowance>(SilentException.UnsupportedBlockchain(chain))
                        .doOnSubscribe {
                            log.error("Allowance for $chain is not supported")
                        }
                }
            } catch (t: Throwable) {
                log.error("Internal error during Allowance Request", t)
                failMetric.increment()
                Flux.error<BlockchainOuterClass.AddressAllowance>(IllegalStateException("Internal Error"))
            }
        }
    }

    override fun estimateFee(request: Mono<BlockchainOuterClass.EstimateFeeRequest>): Mono<BlockchainOuterClass.EstimateFeeResponse> {
        return request
            .flatMap {
                val timer = StopWatch.createStarted()
                val chain = Chain.byId(it.chainValue)
                val metrics = chainMetrics.get(chain)
                metrics.estimateFeeMetric.increment()
                estimateFee.estimateFee(it).doFinally {
                    metrics.estimateFeeRespCountMetric.increment()
                    metrics.estimateFeeRespTimeMetric.record(timer.nanoTime, TimeUnit.NANOSECONDS)
                }
            }
            .doOnError { t ->
                log.error("Internal error during Fee Estimation", t)
                failMetric.increment()
            }
            .contextWrite(requestContext.updateFromGrpc())
    }

    override fun describe(request: Mono<BlockchainOuterClass.DescribeRequest>): Mono<BlockchainOuterClass.DescribeResponse> {
        describeMetric.increment()
        return describe.describe(request)
            .doOnError { failMetric.increment() }
            .contextWrite(requestContext.updateFromGrpc())
    }

    override fun subscribeStatus(request: Mono<BlockchainOuterClass.StatusRequest>): Flux<BlockchainOuterClass.ChainStatus> {
        subscribeStatusMetric.increment()
        return subscribeStatus.subscribeStatus(request)
            .doOnError { failMetric.increment() }
            .contextWrite(requestContext.updateFromGrpc())
    }

    class RequestMetrics(chain: Chain) {
        val nativeCallMetric = Counter.builder("request.grpc.request")
            .tag("type", "nativeCall")
            .tag("chain", chain.chainCode)
            .register(Metrics.globalRegistry)
        val nativeCallRespTimeMetric = Timer.builder("request.grpc.response.time")
            .tag("type", "nativeCall")
            .tag("chain", chain.chainCode)
            .publishPercentileHistogram()
            .register(Metrics.globalRegistry)
        val nativeCallRespCountMetric = Counter.builder("request.grpc.response")
            .tag("type", "nativeCall")
            .tag("chain", chain.chainCode)
            .register(Metrics.globalRegistry)
        val nativeCallErrRespMetric = Counter.builder("request.grpc.response.err")
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
        val getBalanceRespTimeMetric = Timer.builder("request.grpc.response.time")
            .tag("type", "getBalance")
            .tag("chain", chain.chainCode)
            .publishPercentileHistogram()
            .register(Metrics.globalRegistry)
        val getBalanceRespCountMetric = Counter.builder("request.grpc.response")
            .tag("type", "getBalance")
            .tag("chain", chain.chainCode)
            .register(Metrics.globalRegistry)
        val getAddressAllowanceMetric = Counter.builder("request.grpc.request")
            .tag("type", "getAllowance")
            .tag("chain", chain.chainCode)
            .register(Metrics.globalRegistry)
        val getAddressAllowanceRespCountMetric = Counter.builder("request.grpc.response")
            .tag("type", "getAllowance")
            .tag("chain", chain.chainCode)
            .register(Metrics.globalRegistry)
        val getAddressAllowanceRespTimeMetric = Timer.builder("request.grpc.response.time")
            .tag("type", "getAllowance")
            .tag("chain", chain.chainCode)
            .publishPercentileHistogram()
            .register(Metrics.globalRegistry)
        val subscribeAddressAllowanceMetric = Counter.builder("request.grpc.request")
            .tag("type", "subscribeAllowance")
            .tag("chain", chain.chainCode)
            .register(Metrics.globalRegistry)
        val subscribeAddressAllowanceRespMetric = Counter.builder("request.grpc.response")
            .tag("type", "subscribeAllowance")
            .tag("chain", chain.chainCode)
            .register(Metrics.globalRegistry)
        val estimateFeeMetric = Counter.builder("request.grpc.request")
            .tag("type", "estimateFee")
            .tag("chain", chain.chainCode)
            .register(Metrics.globalRegistry)
        val estimateFeeRespTimeMetric = Timer.builder("request.grpc.response.time")
            .tag("type", "estimateFee")
            .tag("chain", chain.chainCode)
            .publishPercentileHistogram()
            .register(Metrics.globalRegistry)
        val estimateFeeRespCountMetric = Counter.builder("request.grpc.response")
            .tag("type", "estimateFee")
            .tag("chain", chain.chainCode)
            .register(Metrics.globalRegistry)
    }
}
