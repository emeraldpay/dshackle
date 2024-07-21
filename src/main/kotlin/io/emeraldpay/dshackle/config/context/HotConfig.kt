package io.emeraldpay.dshackle.config.context

import io.emeraldpay.dshackle.config.hot.AutoReloadbleConfig
import io.emeraldpay.dshackle.config.hot.CompatibleVersionsRules
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.io.ClassPathResource
import java.util.function.Supplier

@Configuration
open class HotConfig {
    @Bean
    open fun hotVersionsConfig(
        @Value("\${compatibility.url}")
        url: String,
        @Value("\${compatibility.enabled}")
        enabled: Boolean,
    ): Supplier<CompatibleVersionsRules?> {
        return if (enabled) {
            val initialContent =
                ClassPathResource("public/compatible-clients.yaml").inputStream.readBytes().toString(Charsets.UTF_8)
            AutoReloadbleConfig(initialContent, url, CompatibleVersionsRules::class.java).also { it.start() }
        } else {
            Supplier { null }
        }
    }
}
