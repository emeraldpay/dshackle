package io.emeraldpay.dshackle.cache

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.domain.BlockHash
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.json.TransactionRefJson
import io.lettuce.core.api.reactive.RedisReactiveCommands
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import java.time.Instant
import java.util.concurrent.TimeUnit
import kotlin.math.min

/**
 * Cache blocks in Redis database
 */
class BlocksRedisCache(
        private val redis: RedisReactiveCommands<String, String>,
        private val chain: Chain,
        private val objectMapper: ObjectMapper
): Reader<BlockHash, BlockJson<TransactionRefJson>> {

    companion object {
        private val log = LoggerFactory.getLogger(BlocksRedisCache::class.java)
        // max caching time is 24 hours
        private const val MAX_CACHE_TIME_HOURS = 24L
    }

    override fun read(key: BlockHash): Mono<BlockJson<TransactionRefJson>> {
        return redis.get(key(key))
                .map { data ->
                    objectMapper.readValue(data, BlockJson::class.java) as BlockJson<TransactionRefJson>
                }.onErrorResume {
                    Mono.empty()
                }
    }

    /**
     * Add to cache.
     * Note that it returns Mono<Void> which must be subscribed to actually save
     */
    open fun add(block: BlockJson<TransactionRefJson>): Mono<Void> {
        if (block.timestamp == null || block.hash == null) {
            return Mono.empty()
        }
        return Mono.just(block)
                .flatMap {
                    val data = objectMapper.writeValueAsString(block)
                    //default caching time is age of the block, i.e. block create hour ago
                    //keep for hour, but block create 10 seconds ago cache for 10 seconds, as it
                    //still can be replaced in the blockchain
                    val age = Instant.now().epochSecond - block.timestamp.epochSecond
                    val ttl = min(age, TimeUnit.HOURS.toSeconds(MAX_CACHE_TIME_HOURS))
                    redis.setex(key(block.hash), ttl, data)
                }
                .doOnError {
                    log.warn("Failed to save to Redis: ${it.message}")
                }
                //if failed to cache, just continue without it
                .onErrorResume {
                    Mono.empty()
                }
                .then()
    }

    /**
     * Key in Redis
     */
    open fun key(hash: BlockHash): String {
        return "block:${chain.id}:${hash.toHex()}"
    }
}