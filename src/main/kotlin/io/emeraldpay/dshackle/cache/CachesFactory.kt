package io.emeraldpay.dshackle.cache

import com.fasterxml.jackson.databind.ObjectMapper
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
        @Autowired private val env: Environment
) {

    companion object {
        private val log = LoggerFactory.getLogger(CachesFactory::class.java)
        private const val CONFIG_PREFIX = "cache.redis"
    }

    private var redis: StatefulRedisConnection<String, String>? = null
    private val all = EnumMap<Chain, Caches>(io.emeraldpay.grpc.Chain::class.java)

    @PostConstruct
    fun init() {
        if (!env.getProperty("${CONFIG_PREFIX}.enabled", Boolean::class.java, false)) {
            return
        }
        val address = env.getProperty("${CONFIG_PREFIX}.host", "127.0.0.1")
        val port = env.getProperty("${CONFIG_PREFIX}.port", Int::class.java, 6379)

        var uri = RedisURI.builder()
                .withHost(address)
                .withPort(port)

        env.getProperty("${CONFIG_PREFIX}.db", Int::class.java)?.let { value ->
            uri = uri.withDatabase(value)
        }

        //log URI _before_ adding a password, to avoid leaking it to the log
        log.info("Use Redis cache at: ${uri.build().toURI()}")

        env.getProperty("${CONFIG_PREFIX}.password")?.let { value ->
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