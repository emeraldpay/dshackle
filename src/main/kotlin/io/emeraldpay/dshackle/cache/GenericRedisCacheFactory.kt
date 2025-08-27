package io.emeraldpay.dshackle.cache

import io.emeraldpay.api.Chain
import io.lettuce.core.api.reactive.RedisReactiveCommands
import org.slf4j.LoggerFactory
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

class GenericRedisCacheFactory(
    private val redis: RedisReactiveCommands<String, ByteArray>,
    private val blockchain: Chain,
) {
    companion object {
        private val log = LoggerFactory.getLogger(GenericRedisCacheFactory::class.java)
    }

    private val setupLock = ReentrantReadWriteLock()
    private val current = mutableMapOf<String, GenericRedisCache>()

    fun get(type: String): GenericRedisCache {
        setupLock.read {
            val existing = current[type]
            if (existing != null) {
                return existing
            }
        }
        setupLock.write {
            val existing = current[type]
            if (existing != null) {
                return existing
            }
            val created = GenericRedisCache(blockchain, type, redis)
            current[type] = created
            return created
        }
    }
}
