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

import io.grpc.Server
import io.grpc.netty.GrpcSslContexts
import io.grpc.netty.NettyServerBuilder
import io.netty.handler.ssl.ClientAuth
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.env.Environment
import org.springframework.core.io.ResourceLoader
import org.springframework.stereotype.Service
import java.io.File
import javax.annotation.PostConstruct
import javax.annotation.PreDestroy

@Service
open class GrpcServer(
        @Autowired val rpcs: List<io.grpc.BindableService>,
        @Autowired val resourceLoader: ResourceLoader,
        @Autowired val env: Environment
) {

    private val log = LoggerFactory.getLogger(GrpcServer::class.java)

    private var server: Server? = null;

    @PostConstruct
    fun start() {
        log.info("Starting GRPC Server...")
        val port = env.getProperty("port", "8090").toInt()
        log.info("Listening on 0.0.0.0:$port")
        val serverBuilder = NettyServerBuilder.forPort(port)
        rpcs.forEach {
            serverBuilder.addService(it)
        }

        val mustBeSecure = env.getProperty("tls.enabled", "") == "true"
        val tlsDisabled = env.getProperty("tls.enabled", "") == "false"
        var hasServerCertificate = true
        if (!tlsDisabled) {
            if (StringUtils.isEmpty(env.getProperty("tls.server.certificate"))) {
                if (mustBeSecure) {
                    log.warn("tls.server.certificate property is not set (path to server TLS certificate)")
                    System.exit(1)
                }
                hasServerCertificate = false
            }
            if (StringUtils.isEmpty(env.getProperty("tls.server.key"))) {
                if (mustBeSecure) {
                    log.warn("tls.server.key property is not set (path to server TLS certificate key)")
                    System.exit(1)
                }
                hasServerCertificate = false
            }
        }
        if (mustBeSecure || (!tlsDisabled && hasServerCertificate)) {
            log.info("Using TLS")
            val sslContextBuilder = GrpcSslContexts.forServer(
                    File(env.getProperty("tls.server.certificate")!!),
                    File(env.getProperty("tls.server.key")!!)
            )
            if (StringUtils.isNotEmpty(env.getProperty("tls.client.ca"))) {
                log.info("Using TLS for client authentication")
                sslContextBuilder.trustManager(
                        File(env.getProperty("tls.client.ca")!!)
                )
                if (env.getProperty("tls.client.require", "true") == "true") {
                    sslContextBuilder.clientAuth(ClientAuth.REQUIRE)
                }
            } else {
                log.warn("Trust all clients")
            }
            serverBuilder.sslContext(sslContextBuilder.build())
        } else {
            log.warn("Using insecure transport")
        }

        val server = serverBuilder.build()
        this.server = server

        Thread { server.start() }.run()
        log.info("GRPC Server started")
    }

    @PreDestroy
    fun stop() {
        log.info("Shutting down GRPC Server...")
        server?.shutdownNow()
        log.info("GRPC Server shot down")
    }
}