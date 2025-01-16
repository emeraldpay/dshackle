package io.emeraldpay.dshackle.config.context

import io.emeraldpay.dshackle.config.MonitoringConfig
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.concurrent.CustomizableThreadFactory
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers
import java.util.concurrent.Executor
import java.util.concurrent.ExecutorService
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Configuration
open class SchedulersConfig {

    companion object {
        private val log = LoggerFactory.getLogger(SchedulersConfig::class.java)
        val threadsMultiplier = run {
            val cores = Runtime.getRuntime().availableProcessors()
            if (cores < 3) {
                1
            } else {
                cores / 2
            }
        }.also { log.info("Creating schedulers with multiplier: {}...", it) }
    }

    @Bean
    open fun rpcScheduler(monitoringConfig: MonitoringConfig): Scheduler {
        return makeScheduler("blockchain-rpc-scheduler", 20, monitoringConfig)
    }

    @Bean
    open fun headScheduler(monitoringConfig: MonitoringConfig): Scheduler {
        return makeScheduler("head-scheduler", 4, monitoringConfig)
    }

    @Bean
    open fun subScheduler(monitoringConfig: MonitoringConfig): Scheduler {
        return makeScheduler("sub-scheduler", 4, monitoringConfig)
    }

    @Bean
    open fun multistreamEventsScheduler(monitoringConfig: MonitoringConfig): Scheduler {
        return makeScheduler("events-scheduler", 4, monitoringConfig)
    }

    @Bean
    open fun wsConnectionResubscribeScheduler(monitoringConfig: MonitoringConfig): Scheduler {
        return makeScheduler("ws-connection-resubscribe-scheduler", 2, monitoringConfig)
    }

    @Bean
    open fun wsScheduler(monitoringConfig: MonitoringConfig): Scheduler {
        return makeScheduler("ws-scheduler", 4, monitoringConfig)
    }

    @Bean
    open fun headLivenessScheduler(monitoringConfig: MonitoringConfig): Scheduler {
        return makeScheduler("head-liveness-scheduler", 4, monitoringConfig)
    }

    @Bean
    open fun grpcChannelExecutor(monitoringConfig: MonitoringConfig): Executor {
        return makePool("grpc-client-channel", 10, monitoringConfig)
    }

    @Bean
    open fun authScheduler(monitoringConfig: MonitoringConfig): Scheduler {
        return makeScheduler("auth-scheduler", 4, monitoringConfig)
    }

    private fun makeScheduler(name: String, size: Int, monitoringConfig: MonitoringConfig): Scheduler {
        return Schedulers.fromExecutorService(makePool(name, size, monitoringConfig))
    }

    private fun makePool(name: String, size: Int, monitoringConfig: MonitoringConfig): ExecutorService {
        val cachedPool = ThreadPoolExecutor(
            size,
            size * threadsMultiplier,
            60L,
            TimeUnit.SECONDS,
            LinkedBlockingQueue(1000),
            CustomizableThreadFactory("$name-"),
        )

        return if (monitoringConfig.enableExtended) {
            ExecutorServiceMetrics.monitor(
                Metrics.globalRegistry,
                cachedPool,
                name,
                Tag.of("reactor_scheduler_id", "_"),
            )
        } else {
            cachedPool
        }
    }
}
