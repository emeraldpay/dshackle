package io.emeraldpay.dshackle.config

class CacheConfig {

    var redis: Redis? = null;

    class Redis(
            var host: String = "127.0.0.1",
            var port: Int = 6379,
            var db: Int? = 0,
            var password: String? = null
    )
}