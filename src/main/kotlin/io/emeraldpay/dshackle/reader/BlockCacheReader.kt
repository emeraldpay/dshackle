package io.emeraldpay.dshackle.reader

import io.emeraldpay.dshackle.cache.BlocksMemCache
import io.infinitape.etherjar.domain.BlockHash
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.json.BlockJson
import reactor.core.publisher.Mono

class BlockCacheReader(
        val cache: BlocksMemCache
): Reader<BlockHash, BlockJson<TransactionId>> {

    override fun read(key: BlockHash): Mono<BlockJson<TransactionId>> {
        return cache.get(key)
    }
}