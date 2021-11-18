/**
 * Copyright (c) 2020 EmeraldPay, Inc
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

import io.emeraldpay.dshackle.config.CacheConfig
import io.emeraldpay.dshackle.config.HealthConfig
import io.emeraldpay.dshackle.config.MainConfig
import io.emeraldpay.dshackle.config.MainConfigReader
import io.emeraldpay.dshackle.config.MonitoringConfig
import io.emeraldpay.dshackle.config.TokensConfig
import io.emeraldpay.dshackle.config.UpstreamsConfig
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.ExitCodeGenerator
import org.springframework.boot.SpringApplication
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.env.Environment
import org.springframework.scheduling.annotation.EnableAsync
import org.springframework.scheduling.annotation.EnableScheduling
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers
import java.io.File
import java.util.concurrent.Executors
import kotlin.system.exitProcess

@Configuration
@EnableScheduling
@EnableAsync
open class Config(
    @Autowired private val env: Environment,
    @Autowired private val ctx: ApplicationContext
) {

    companion object {
        private val log = LoggerFactory.getLogger(Config::class.java)

        private const val DEFAULT_CONFIG = "/etc/dshackle/dshackle.yaml"
        private const val LOCAL_CONFIG = "./dshackle.yaml"
    }

    private var configFilePath: File? = null

    init {
        configFilePath = getConfigPath()
        Global.version = env.getProperty("version.app", Global.version).let {
            if (it.contains("SNAPSHOT")) {
                listOfNotNull(it, env.getProperty("version.commit")).joinToString("-")
            } else {
                it
            }
        }
    }

    fun getConfigPath(): File {
        env.getProperty("configPath")?.let {
            return File(it).normalize()
        }
        var target = File(DEFAULT_CONFIG)
        if (!FileResolver.isAccessible(target)) {
            target = File(LOCAL_CONFIG)
            if (!FileResolver.isAccessible(target)) {
                throw IllegalStateException("Configuration is not found neither at $DEFAULT_CONFIG nor $LOCAL_CONFIG")
            }
        }
        target = target.normalize()
        return target
    }

    @Bean
    @Qualifier("upstreamScheduler")
    open fun upstreamScheduler(): Scheduler {
        return Schedulers.fromExecutorService(Executors.newFixedThreadPool(16))
    }

    @Bean
    open fun mainConfig(@Autowired fileResolver: FileResolver): MainConfig {
        val f = configFilePath ?: throw IllegalStateException("Config path is not set")
        log.info("Using config: ${f.absolutePath}")
        if (!f.exists() || !f.isFile) {
            log.error("Config doesn't exist or not a file: ${f.absolutePath}")
            SpringApplication.exit(ctx, ExitCodeGenerator { 1 })
            exitProcess(1)
        }
        val reader = MainConfigReader(fileResolver)
        return reader.read(f.inputStream())
            ?: throw IllegalStateException("Config is not available at ${f.absolutePath}")
    }

    @Bean
    open fun fileResolver(): FileResolver {
        val f = configFilePath ?: throw IllegalStateException("Config path is not set")
        return FileResolver(f.absoluteFile.parentFile)
    }

    @Bean
    open fun upstreamsConfig(@Autowired mainConfig: MainConfig): UpstreamsConfig? {
        return mainConfig.upstreams
    }

    @Bean
    open fun cacheConfig(@Autowired mainConfig: MainConfig): CacheConfig {
        return mainConfig.cache ?: CacheConfig()
    }

    @Bean
    open fun tokensConfig(@Autowired mainConfig: MainConfig): TokensConfig {
        return mainConfig.tokens ?: TokensConfig(emptyList())
    }

    @Bean
    open fun monitoringConfig(@Autowired mainConfig: MainConfig): MonitoringConfig {
        return mainConfig.monitoring
    }

    @Bean
    open fun healthConfig(@Autowired mainConfig: MainConfig): HealthConfig {
        return mainConfig.health
    }
}
