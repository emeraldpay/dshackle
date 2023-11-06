package io.emeraldpay.dshackle.config.context

import io.emeraldpay.dshackle.config.MonitoringConfig
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Tag
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
        return makeScheduler("blockchain-rpc-scheduler", 30, monitoringConfig)
    }

    @Bean
    open fun trackTxScheduler(monitoringConfig: MonitoringConfig): Scheduler {
        return makeScheduler("tracktx-scheduler", 5, monitoringConfig)
    }

    @Bean
    open fun headScheduler(monitoringConfig: MonitoringConfig): Scheduler {
        return makeScheduler("head-scheduler", 4, monitoringConfig)
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
        val pool = Executors.newFixedThreadPool(size, CustomizableThreadFactory("$name-"))

        return if (monitoringConfig.enableExtended) {
            ExecutorServiceMetrics.monitor(
                Metrics.globalRegistry,
                pool,
                name,
                Tag.of("reactor_scheduler_id", "_"),
            )
        } else {
            pool
        }
    }
}
