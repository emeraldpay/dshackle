/**
 * Copyright (c) 2019 ETCDEV GmbH
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
package io.emeraldpay.dshackle

import com.fasterxml.jackson.core.Version
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import org.springframework.core.env.Environment
import org.springframework.scheduling.annotation.EnableAsync
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.annotation.Scheduled
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers
import java.io.File
import java.lang.IllegalStateException
import java.text.SimpleDateFormat
import java.util.*
import java.util.concurrent.Executors
import kotlin.system.exitProcess

@Configuration
@EnableScheduling
@EnableAsync
open class Config(
        @Autowired private val env: Environment
) {

    private val log = LoggerFactory.getLogger(Config::class.java)

    @Bean
    open fun objectMapper(): ObjectMapper {
        val module = SimpleModule("EmeraldDShackle", Version(1, 0, 0, null, null, null))

        val objectMapper = ObjectMapper()
        objectMapper.registerModule(module)
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        objectMapper
                .setDateFormat(SimpleDateFormat("yyyy-MM-dd\'T\'HH:mm:ss.SSS"))
                .setTimeZone(TimeZone.getTimeZone("UTC"))

        return objectMapper
    }

    @Bean @Qualifier("upstreamScheduler")
    open fun upstreamScheduler(): Scheduler {
        return Schedulers.fromExecutorService(Executors.newFixedThreadPool(16))
    }

    @Bean
    @Qualifier("configDir")
    open fun configDir(): File {
        val config = env.getProperty("configPath") ?: throw IllegalStateException("Config path is not set")
        if (config.trim().isEmpty()) {
            throw IllegalStateException("Config path is empty")
        }
        log.info("Use configuration from: $config")
        return File(config).parentFile
    }

    @Bean
    open fun fileResolver(): FileResolver {
        return FileResolver(configDir())
    }
}