package io.emeraldpay.dshackle

import com.github.benmanes.caffeine.cache.Caffeine
import org.slf4j.Logger
import java.time.Duration

class ThrottledLogger {
    companion object {

        private val cache = Caffeine.newBuilder()
            .expireAfterWrite(Duration.ofMinutes(20))
            .build<String, Boolean>()

        fun log(log: Logger, msg: String) {
            log(log, msg, msg)
        }

        fun log(log: Logger, category: String, msg: String) {
            if (cache.getIfPresent(category) == null) {
                log.warn(msg)
                cache.put(category, true)
            }
        }
    }
}
