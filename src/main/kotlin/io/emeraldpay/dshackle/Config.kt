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
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.annotation.EnableAsync
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.annotation.Scheduled
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers
import java.text.SimpleDateFormat
import java.util.*
import java.util.concurrent.Executors

@Configuration
@EnableScheduling
@EnableAsync
open class Config {

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

}