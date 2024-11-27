/**
 * Copyright (c) 2020 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.cache

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.config.CacheConfig
import io.lettuce.core.RedisClient
import io.lettuce.core.RedisConnectionException
import io.lettuce.core.RedisURI
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.codec.ByteArrayCodec
import io.lettuce.core.codec.RedisCodec
import io.lettuce.core.codec.StringCodec
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import java.util.EnumMap
import javax.annotation.PostConstruct
import kotlin.system.exitProcess

@Repository
open class CachesFactory(
    @Autowired private val cacheConfig: CacheConfig,
) {

    companion object {
        private val log = LoggerFactory.getLogger(CachesFactory::class.java)
        private const val CONFIG_PREFIX = "cache.redis"
    }

    private var redis: StatefulRedisConnection<String, ByteArray>? = null
    private val all = EnumMap<Chain, Caches>(Chain::class.java)

    @PostConstruct
    fun init() {
        val redisConfig = cacheConfig.redis ?: return

        var uri = RedisURI.builder()
            .withHost(redisConfig.host)
            .withPort(redisConfig.port)

        redisConfig.db?.let { value ->
            uri = uri.withDatabase(value)
        }

        // log URI _before_ adding a password, to avoid leaking it to the log
        log.info("Use Redis cache at: ${uri.build().toURI()}")

        redisConfig.password?.let { value ->
            uri = uri.withPassword(value)
        }

        val client = RedisClient.create(uri.build())
        try {
            val ping = client.connect().sync().ping()
            if (ping != "PONG") {
                throw IllegalStateException("Redis connection is not configured. Response: $ping")
            }
            log.info("Connection to Redis established")
            redis = client.connect(RedisCodec.of(StringCodec.ASCII, ByteArrayCodec.INSTANCE))
        } catch (e: RedisConnectionException) {
            log.error("Unable to establish connection to the Redis server")
            log.error("Redis error: ${e.message}")
            log.error("Redis config: ")
            log.error("  uri: ${redisConfig.host}:${redisConfig.port}")
            redisConfig.db?.let {
                log.error("  db: $it")
            }
            if (redisConfig.password != null && redisConfig.password!!.isNotBlank()) {
                log.error("  password: (set)")
            } else {
                log.error("  password: (not set)")
            }
            log.error("Stopping the server...")
            exitProcess(1)
        }
    }

    private fun initCache(chain: Chain): Caches {
        val caches = Caches.newBuilder()
        if (chain == Chain.ZIRCUIT__MAINNET || chain == Chain.ZIRCUIT__TESTNET) {
            caches.setCacheEnabled(false)
        }
        redis?.let { redis ->
            caches.setBlockByHash(BlocksRedisCache(redis.reactive(), chain))
            caches.setTxByHash(TxRedisCache(redis.reactive(), chain))
            caches.setReceipts(ReceiptRedisCache(redis.reactive(), chain))
            caches.setHeightByHash(HeightByHashRedisCache(redis.reactive(), chain))
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
