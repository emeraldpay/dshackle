package io.emeraldpay.dshackle.upstream.error

import io.emeraldpay.dshackle.config.context.SchedulersConfig
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.Upstream
import org.springframework.scheduling.concurrent.CustomizableThreadFactory
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors

interface ErrorHandler {
    fun handle(upstream: Upstream, request: ChainRequest, errorMessage: String?)

    fun canHandle(request: ChainRequest, errorMessage: String?): Boolean
}

object UpstreamErrorHandler {
    private val errorHandlers = listOf(
        EthereumStateLowerBoundErrorHandler,
        EthereumTraceLowerBoundErrorHandler,
    )
    private val errorHandlerExecutor = Executors.newFixedThreadPool(
        2 * SchedulersConfig.threadsMultiplier,
        CustomizableThreadFactory("error-handler-"),
    )

    fun handle(upstream: Upstream, request: ChainRequest, errorMessage: String?) {
        CompletableFuture.runAsync(
            {
                errorHandlers
                    .filter { it.canHandle(request, errorMessage) }
                    .forEach { it.handle(upstream, request, errorMessage) }
            },
            errorHandlerExecutor,
        )
    }
}
