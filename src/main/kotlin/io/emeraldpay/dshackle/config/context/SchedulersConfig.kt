package io.emeraldpay.dshackle.config.context

import io.emeraldpay.dshackle.config.MonitoringConfig
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.concurrent.CustomizableThreadFactory
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers
import java.util.concurrent.Executor
import java.util.concurrent.ExecutorService
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

    @Bean
    open fun grpcChannelExecutor(monitoringConfig: MonitoringConfig): Executor {
        return makePool("grpc-client-channel", "grpc_client_channel", 10, monitoringConfig)
    }

    private fun makeScheduler(name: String, prefix: String, size: Int, monitoringConfig: MonitoringConfig): Scheduler {
        return Schedulers.fromExecutorService(makePool(name, prefix, size, monitoringConfig))
    }

    private fun makePool(name: String, prefix: String, size: Int, monitoringConfig: MonitoringConfig): ExecutorService {
        val pool = Executors.newFixedThreadPool(size, CustomizableThreadFactory("$name-"))

        return if (monitoringConfig.enableExtended)
            ExecutorServiceMetrics.monitor(
                Metrics.globalRegistry,
                pool,
                name,
                prefix
            )
        else
            pool
    }
}
