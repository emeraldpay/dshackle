package io.emeraldpay.dshackle.config.hot

import io.emeraldpay.dshackle.Global
import org.slf4j.LoggerFactory
import org.springframework.web.client.RestTemplate
import reactor.core.publisher.Flux
import java.time.Duration
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Supplier

class AutoReloadbleConfig<T>(
    val initialContent: String,
    val configUrl: String,
    private val type: Class<T>,
) : Supplier<T?> {
    companion object {
        private val log = LoggerFactory.getLogger(AutoReloadbleConfig::class.java)
    }

    private val restTemplate = RestTemplate()
    private val instance = AtomicReference<T>()

    fun reload() {
        try {
            val response = restTemplate.getForObject(configUrl, String::class.java)
            if (response != null) {
                instance.set(parseConfig(response))
            }
        } catch (e: Exception) {
            log.error("Failed to reload config from $configUrl for type $type", e)
        }
    }

    fun start() {
        instance.set(parseConfig(initialContent))
        Flux.interval(
            Duration.ofSeconds(600),
        ).subscribe {
            reload()
        }
    }

    private fun parseConfig(config: String): T {
        return Global.yamlMapper.readValue(config, type)
    }

    override fun get(): T {
        return instance.get()
    }
}
