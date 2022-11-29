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
import io.grpc.Server
import io.grpc.netty.NettyServerBuilder
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import java.net.InetSocketAddress
import javax.annotation.PostConstruct
import javax.annotation.PreDestroy

@Service
open class GrpcServer(
    @Autowired val rpcs: List<io.grpc.BindableService>,
    @Autowired val mainConfig: MainConfig,
    @Autowired val tlsSetup: TlsSetup,
    @Autowired val accessHandler: AccessHandlerGrpc
) {

    private val log = LoggerFactory.getLogger(GrpcServer::class.java)

    private var server: Server? = null

    @PostConstruct
    fun start() {
        log.info("Starting gRPC Server...")
        log.debug("Running with DEBUG LOGGING")
        log.info("Listening Native gRPC on ${mainConfig.host}:${mainConfig.port}")
        val serverBuilder = NettyServerBuilder
            .forAddress(InetSocketAddress(mainConfig.host, mainConfig.port))
            .maxInboundMessageSize(Int.MAX_VALUE)
            .let {
                if (mainConfig.accessLogConfig.enabled) {
                    it.intercept(accessHandler)
                } else {
                    it
                }
            }

        tlsSetup.setupServer("Native gRPC", mainConfig.tls, true)?.let {
            serverBuilder.sslContext(it)
        }

        rpcs.forEach {
            serverBuilder.addService(it)
        }

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
