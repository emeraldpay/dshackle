package io.emeraldpay.dshackle.cache

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.config.CacheConfig
import io.emeraldpay.dshackle.config.EnvVariables
import io.emeraldpay.grpc.Chain
import io.lettuce.core.RedisClient
import io.lettuce.core.RedisURI
import io.lettuce.core.api.StatefulRedisConnection
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.core.env.Environment
import org.springframework.stereotype.Repository
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import javax.annotation.PostConstruct
import kotlin.collections.HashMap


@Repository
class CachesFactory(
        @Autowired private val objectMapper: ObjectMapper,
        @Autowired private val cacheConfig: CacheConfig
) {

    companion object {
        private val log = LoggerFactory.getLogger(CachesFactory::class.java)
        private const val CONFIG_PREFIX = "cache.redis"
    }

    private var redis: StatefulRedisConnection<String, String>? = null
    private val all = EnumMap<Chain, Caches>(io.emeraldpay.grpc.Chain::class.java)

    @PostConstruct
    fun init() {
        val redisConfig = cacheConfig.redis ?: return

        var uri = RedisURI.builder()
                .withHost(redisConfig.host)
                .withPort(redisConfig.port)

        redisConfig.db?.let { value ->
            uri = uri.withDatabase(value)
        }

        //log URI _before_ adding a password, to avoid leaking it to the log
        log.info("Use Redis cache at: ${uri.build().toURI()}")

        redisConfig.password?.let { value ->
            uri = uri.withPassword(value)
        }

        val client = RedisClient.create(uri.build())
        val ping = client.connect().sync().ping()
        if (ping != "PONG") {
            throw IllegalStateException("Redis connection is not configured. Response: $ping")
        }
        redis = client.connect()
    }

    private fun initCache(chain: Chain): Caches {
        val caches = Caches.newBuilder()
                .setObjectMapper(objectMapper)
        redis?.let { redis ->
            caches.setBlockByHash(BlocksRedisCache(redis.reactive(), chain, objectMapper))
            caches.setTxByHash(TxRedisCache(redis.reactive(), chain, objectMapper))
        }
        return caches.build()
    }

    fun getCaches(chain: Chain): Caches {
        val existing = all[chain]
        if (existing == null) {
            synchronized(all) {
                if (!all.containsKey(chain)) {
                    all[chain] = initCache(chain)
                }
            }
            return getCaches(chain)
        }
        return existing
    }
}