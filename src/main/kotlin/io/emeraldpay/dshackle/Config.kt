package io.emeraldpay.dshackle

import com.fasterxml.jackson.core.Version
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.annotation.EnableAsync
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.annotation.Scheduled
import java.text.SimpleDateFormat
import java.util.*

@Configuration
@EnableScheduling
@EnableAsync
open class Config {

    @Bean
    open fun objectMapper(): ObjectMapper {
        val module = SimpleModule("EmeraldDShackle", Version(1, 0, 0, null, null, null))

        val objectMapper = ObjectMapper()
        objectMapper.registerModule(module)
        objectMapper
                .setDateFormat(SimpleDateFormat("yyyy-MM-dd\'T\'HH:mm:ss.SSS"))
                .setTimeZone(TimeZone.getTimeZone("UTC"))

        return objectMapper
    }

}