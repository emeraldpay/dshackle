package io.emeraldpay.dshackle.reader

import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.Upstream
import io.infinitape.etherjar.domain.BlockHash
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.Commands
import io.infinitape.etherjar.rpc.json.BlockJson
import reactor.core.publisher.Mono
import reactor.retry.Repeat
import java.time.Duration

class BlockApiReader(
        val upstream: Upstream
): Reader<BlockHash, BlockJson<TransactionId>> {

    override fun read(key: BlockHash): Mono<BlockJson<TransactionId>> {
        return Mono.just(key)
                .flatMap {
                    upstream.getApi(Selector.empty).executeAndConvert(Commands.eth().getBlock(it))
                }.repeatWhenEmpty { n ->
                    Repeat.times<Any>(3)
                            .exponentialBackoff(Duration.ofMillis(100), Duration.ofMillis(500))
                            .apply(n)
                }
    }
}