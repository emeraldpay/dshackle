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

import io.emeraldpay.dshackle.config.MainConfig
import io.emeraldpay.dshackle.monitoring.accesslog.AccessHandlerGrpc
import io.grpc.Codec
import io.grpc.Server
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.ServerInterceptor
import io.grpc.netty.NettyServerBuilder
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics
import org.slf4j.LoggerFactory
import org.springframework.scheduling.concurrent.CustomizableThreadFactory
import org.springframework.stereotype.Service
import java.net.InetSocketAddress
import java.util.concurrent.Executors
import javax.annotation.PostConstruct
import javax.annotation.PreDestroy

@Service
open class GrpcServer(
    private val rpcs: List<io.grpc.BindableService>,
    private val mainConfig: MainConfig,
    private val tlsSetup: TlsSetup,
    private val accessHandler: AccessHandlerGrpc,
    private val grpcServerBraveInterceptor: ServerInterceptor
) {

    private val log = LoggerFactory.getLogger(GrpcServer::class.java)

    private var server: Server? = null

    class CompressionInterceptor : ServerInterceptor {
        override fun <ReqT : Any, RespT : Any> interceptCall(
            call: ServerCall<ReqT, RespT>,
            headers: io.grpc.Metadata,
            next: ServerCallHandler<ReqT, RespT>
        ): ServerCall.Listener<ReqT> {
            call.setCompression(Codec.Gzip().messageEncoding)
            return next.startCall(call, headers)
        }
    }

    @PostConstruct
    fun start() {
        log.info("Starting gRPC Server...")
        log.debug("Running with DEBUG LOGGING")
        log.info("Listening Native gRPC on ${mainConfig.host}:${mainConfig.port}")
        val serverBuilder = NettyServerBuilder
            .forAddress(InetSocketAddress(mainConfig.host, mainConfig.port))
            .maxInboundMessageSize(Defaults.maxMessageSize)
            .let {
                if (mainConfig.accessLogConfig.enabled) {
                    it.intercept(accessHandler)
                }
                if (mainConfig.compression.grpc.serverEnabled) {
                    it.intercept(CompressionInterceptor())
                    log.info("Compression enabled for gRPC server")
                }
                it
            }

        serverBuilder.intercept(grpcServerBraveInterceptor)

        tlsSetup.setupServer("Native gRPC", mainConfig.tls, true)?.let {
            serverBuilder.sslContext(it)
        }

        rpcs.forEach {
            serverBuilder.addService(it)
        }

        val pool = Executors.newFixedThreadPool(20, CustomizableThreadFactory("fixed-grpc-"))

        serverBuilder.executor(
            if (mainConfig.monitoring.enableExtended)
                ExecutorServiceMetrics.monitor(
                    Metrics.globalRegistry,
                    pool,
                    "fixed-grpc-executor",
                    "grpc_"
                )
            else
                pool
        )

        val server = serverBuilder.build()
        this.server = server

        server.start()
        log.info("GRPC Server started")
    }

    @PreDestroy
    fun stop() {
        log.info("Shutting down GRPC Server...")
        server?.shutdownNow()
        log.info("GRPC Server shot down")
    }
}
