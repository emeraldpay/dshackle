package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.Defaults
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.etherjar.domain.BlockHash
import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler
import reactor.retry.Repeat
import java.time.Duration

class EthereumBlockEnricher {
    companion object {
        fun enrich(blockHash: BlockHash, api: Reader<BlockHash, BlockContainer>, scheduler: Scheduler): Mono<BlockContainer> {
            return Mono.just(blockHash)
                .flatMap { hash ->
                    api.read(hash)
                        .subscribeOn(scheduler)
                        .timeout(Defaults.timeoutInternal, Mono.empty())
                }.repeatWhenEmpty { n ->
                    Repeat.times<Any>(5)
                        .exponentialBackoff(Duration.ofMillis(50), Duration.ofMillis(500))
                        .apply(n)
                }
                .timeout(Defaults.timeout, Mono.empty())
                .onErrorResume { Mono.empty() }
        }
    }
}
