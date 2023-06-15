package io.emeraldpay.dshackle.cache

import com.google.protobuf.ByteString
import io.emeraldpay.api.Chain
import io.emeraldpay.dshackle.proto.CachesProto
import io.emeraldpay.dshackle.reader.Reader
import io.lettuce.core.api.reactive.RedisReactiveCommands
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import java.time.Duration

class GenericRedisCache(
    private val chain: Chain,
    private val type: String,
    private val redis: RedisReactiveCommands<String, ByteArray>,
) : Reader<String, ByteArray> {

    companion object {
        private val log = LoggerFactory.getLogger(GenericRedisCache::class.java)
        private val DEFAULT_CACHE_TIME = Duration.ofMinutes(60)
        private val MAX_CACHE_TIME = Duration.ofHours(24)
    }

    init {
        require(StringUtils.isAlphanumeric(type)) {
            "Generic Caching Type must be an alpha-numeric value. Received: `$type`"
        }
    }

    override fun read(key: String): Mono<ByteArray> {
        return redis.get(asRedisKey(key))
            .map {
                CachesProto.ValueContainer.parseFrom(it)
            }
            .filter {
                it.type == CachesProto.ValueContainer.ValueType.GENERIC && it.hasGenericMeta() && it.genericMeta.type == type
            }
            .map { data ->
                data.value.toByteArray()
            }
            .onErrorResume { t ->
                log.warn("Failed to read from Redis. ${t.javaClass} ${t.message}")
                Mono.empty()
            }
    }

    fun asRedisKey(key: String): String {
        return "generic:${chain.id}:$type:$key"
    }

    fun put(key: String, value: ByteArray, ttl: Duration?): Mono<Void> {
        return Mono.just(value)
            .flatMap {
                val resultingTtl = (ttl ?: DEFAULT_CACHE_TIME).coerceAtMost(MAX_CACHE_TIME)
                val proto = toProto(it)
                redis.setex(asRedisKey(key), resultingTtl.seconds, proto.toByteArray())
            }
            .doOnError {
                log.warn("Failed to save Generic Data $type to Redis: ${it.message}")
            }
            // if failed to cache, just continue without it
            .onErrorResume {
                Mono.empty()
            }
            .then()
    }

    fun evict(key: String): Mono<Void> {
        return redis.del(asRedisKey(key)).then()
    }

    fun toProto(value: ByteArray): CachesProto.ValueContainer {
        return CachesProto.ValueContainer.newBuilder()
            .setType(CachesProto.ValueContainer.ValueType.GENERIC)
            .setValue(ByteString.copyFrom(value))
            .setGenericMeta(
                CachesProto.GenericMeta.newBuilder()
                    .setType(type)
            )
            .build()
    }
}
