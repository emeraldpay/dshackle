package io.emeraldpay.dshackle.test

import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.codec.ByteArrayCodec
import io.lettuce.core.codec.RedisCodec
import io.lettuce.core.codec.StringCodec

class IntegrationTestingCommons {

    static boolean isEnabled(String name) {
        return System.getenv("DSHACKLE_TEST_ENABLED") != null && System.getenv("DSHACKLE_TEST_ENABLED").contains(name)
    }

    static boolean isDisabled(String name) {
        return !isEnabled(name)
    }

    static String env(String name, String defaultValue) {
        return System.getenv(name) ?: defaultValue
    }

    static RedisClient redis() {
        String host = env("REDIS_HOST", "localhost")
        String port = env("REDIS_PORT", "6379")
        return RedisClient.create("redis://${host}:${port}")
    }

    static StatefulRedisConnection<String, byte[]> redisConnection() {
        return redis().connect(RedisCodec.of(StringCodec.ASCII, ByteArrayCodec.INSTANCE))
    }
}
