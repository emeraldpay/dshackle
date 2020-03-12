package io.emeraldpay.dshackle.cache

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.json.TransactionJson
import io.infinitape.etherjar.rpc.json.TransactionRefJson
import io.lettuce.core.api.reactive.RedisReactiveCommands
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import reactor.util.function.Tuples
import java.time.Instant
import java.util.concurrent.TimeUnit
import kotlin.math.min

/**
 * Cache transactions in Redis, up to 24 hours.
 */
class TxRedisCache(
        private val redis: RedisReactiveCommands<String, String>,
        private val chain: Chain,
        private val objectMapper: ObjectMapper
): Reader<TransactionId, TransactionJson> {

    companion object {
        private val log = LoggerFactory.getLogger(TxRedisCache::class.java)
        // max caching time is 24 hours
        private const val MAX_CACHE_TIME_HOURS = 24L
    }

    override fun read(key: TransactionId): Mono<TransactionJson> {
        return redis.get(key(key))
                .map { data ->
                    objectMapper.readValue(data, TransactionJson::class.java) as TransactionJson
                }.onErrorResume {
                    Mono.empty()
                }
    }

    open fun evict(block: BlockJson<TransactionRefJson>): Mono<Void> {
        return Mono.just(block)
                .map { block ->
                    block.transactions.map {
                        key(it.hash)
                    }.toTypedArray()
                }.flatMap { keys ->
                    redis.del(*keys)
                }.then()
    }


    open fun add(tx: TransactionJson, block: BlockJson<TransactionRefJson>): Mono<Void> {
        if (tx.blockHash == null || block.hash == null || tx.blockHash != block.hash || block.timestamp == null) {
            return Mono.empty()
        }
        return Mono.just(Tuples.of(tx, block))
                .flatMap {
                    val data = objectMapper.writeValueAsString(it.t1)
                    //default caching time is age of the block, i.e. block create hour ago
                    //keep for hour, but block create 10 seconds ago cache for 10 seconds, as it
                    //still can be replaced in the blockchain
                    val age = Instant.now().epochSecond - it.t2.timestamp.epochSecond
                    val ttl = min(age, TimeUnit.HOURS.toSeconds(MAX_CACHE_TIME_HOURS))
                    redis.setex(key(it.t1.hash), ttl, data)
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
    open fun key(hash: TransactionId): String {
        return "tx:${chain.id}:${hash.toHex()}"
    }
}