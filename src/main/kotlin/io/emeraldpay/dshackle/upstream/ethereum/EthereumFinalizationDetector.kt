package io.emeraldpay.dshackle.upstream.ethereum

import com.fasterxml.jackson.module.kotlin.readValue
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Defaults
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.ethereum.json.BlockJson
import io.emeraldpay.dshackle.upstream.ethereum.json.TransactionRefJson
import io.emeraldpay.dshackle.upstream.finalization.FinalizationData
import io.emeraldpay.dshackle.upstream.finalization.FinalizationDetector
import io.emeraldpay.dshackle.upstream.finalization.FinalizationType
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

class EthereumFinalizationDetector : FinalizationDetector {
    companion object {
        private val log = LoggerFactory.getLogger(EthereumFinalizationDetector::class.java)
    }

    val data: ConcurrentHashMap<FinalizationType, FinalizationData> = ConcurrentHashMap()
    private val disableDetector: ConcurrentHashMap<FinalizationType, Boolean> = ConcurrentHashMap()
    private val finalizationSink = Sinks.many().multicast().directBestEffort<FinalizationData>()

    private val errorRegex = "(" +
        ".*bad request.*" +
        "|.*block not found.*" +
        "|.*Unknown block.*" +
        "|.*tag not supported on pre-merge network.*" +
        "|.*hex string without 0x prefix.*" +
        "|.*Invalid params.*" +
        "|.*invalid syntax.*" +
        "|.*invalid block number.*" +
        ")"
    override fun detectFinalization(
        upstream: Upstream,
        blockTime: Duration,
        chain: Chain,
    ): Flux<FinalizationData> {
        return Flux.merge(
            finalizationSink.asFlux(),
            Flux.interval(
                Duration.ofSeconds(0),
                Duration.ofSeconds(15),
            ).flatMap {
                Flux.fromIterable(
                    listOf(
                        Pair(
                            FinalizationType.SAFE_BLOCK,
                            ChainRequest(
                                "eth_getBlockByNumber",
                                ListParams("safe", false),
                                1,
                            ),
                        ),
                        Pair(
                            FinalizationType.FINALIZED_BLOCK,
                            ChainRequest(
                                "eth_getBlockByNumber",
                                ListParams("finalized", false),
                                2,
                            ),
                        ),
                    ),
                ).flatMap { (type, req) ->
                    if (!disableDetector.getOrDefault(type, false)) {
                        upstream
                            .getIngressReader()
                            .read(req)
                            .timeout(Defaults.internalCallsTimeout)
                            .flatMap {
                                it.requireResult().flatMap { result ->
                                    val block = Global.objectMapper.readValue<BlockJson<TransactionRefJson>>(result)
                                    if (block != null) {
                                        Mono.just(FinalizationData(block.number, type))
                                    } else {
                                        Mono.empty()
                                    }
                                }
                            }
                            .onErrorResume {
                                if (it.message != null && it.message!!.matches(Regex(errorRegex))) {
                                    log.warn("Can't retrieve tagged block, finalization detector of upstream {} tag {} is disabled", upstream.getId(), type)
                                    disableDetector[type] = true
                                } else {
                                    log.error("Error in FinalizationDetector of upstream {}, reason - {}", upstream.getId(), it.message)
                                }
                                Mono.empty()
                            }
                    } else {
                        Flux.empty()
                    }
                }
            }.filter {
                it.height > (data[it.type]?.height ?: 0)
            }.doOnNext {
                data[it.type] = it
            },
        )
    }

    override fun addFinalization(finalization: FinalizationData) {
        finalizationSink.emitNext(finalization) { _, res -> res == Sinks.EmitResult.FAIL_NON_SERIALIZED }
    }

    override fun getFinalizations(): Collection<FinalizationData> {
        return data.values
    }
}
