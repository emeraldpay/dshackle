package io.emeraldpay.dshackle.config.context

import io.emeraldpay.dshackle.config.MonitoringConfig
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.concurrent.CustomizableThreadFactory
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers
import java.util.concurrent.Executors

@Configuration
open class SchedulersConfig {
    @Bean
    open fun rpcScheduler(monitoringConfig: MonitoringConfig): Scheduler {
        return makeScheduler("blockchain-rpc-scheduler", "blockchain_rpc", 30, monitoringConfig)
    }

    @Bean
    open fun trackTxScheduler(monitoringConfig: MonitoringConfig): Scheduler {
        return makeScheduler("tracktx-scheduler", "tracktx", 5, monitoringConfig)
    }

    @Bean
    open fun headMergedScheduler(monitoringConfig: MonitoringConfig): Scheduler {
        return makeScheduler("head-scheduler", "head_merge", 5, monitoringConfig)
    }

    private fun makeScheduler(name: String, prefix: String, size: Int, monitoringConfig: MonitoringConfig): Scheduler {
        val pool = Executors.newFixedThreadPool(size, CustomizableThreadFactory("$name-"))

        return Schedulers.fromExecutorService(
            if (monitoringConfig.enableExtended)
                ExecutorServiceMetrics.monitor(
                    Metrics.globalRegistry,
                    pool,
                    name,
                    prefix
                )
            else
                pool
        )
    }
}
