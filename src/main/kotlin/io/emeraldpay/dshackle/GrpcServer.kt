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

        var secure = false
        if (env.getProperty("ssl", "false") == "true") {
            secure = true
            if (StringUtils.isEmpty(env.getProperty("ssl.cert"))) {
                log.warn("ssl.cert property is not set (path to server TLS certificate)")
                secure = false
            }
            if (StringUtils.isEmpty(env.getProperty("ssl.key"))) {
                log.warn("ssl.key property is not set (path to server TLS certificate key)")
                secure = false
            }
        }
        if (secure) {
            log.info("Using TLS")
            val sslContextBuilder = GrpcSslContexts.forServer(
                    File(env.getProperty("ssl.cert")!!),
                    File(env.getProperty("ssl.key")!!)
            )
            if (StringUtils.isNotEmpty(env.getProperty("ssl.client.cert"))) {
                log.info("Using TLS for client authentication")
                sslContextBuilder.trustManager(
                        File(env.getProperty("ssl.client.cert")!!)
                ).clientAuth(ClientAuth.REQUIRE)
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