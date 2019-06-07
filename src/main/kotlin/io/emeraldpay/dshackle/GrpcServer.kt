package io.emeraldpay.dshackle

import io.grpc.Server
import io.grpc.ServerBuilder
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.io.ResourceLoader
import org.springframework.stereotype.Service
import javax.annotation.PostConstruct
import javax.annotation.PreDestroy

@Service
open class GrpcServer(
        @Autowired val rpcs: List<io.grpc.BindableService>,
        @Autowired val resourceLoader: ResourceLoader
) {

    private val log = LoggerFactory.getLogger(GrpcServer::class.java)

    private var server: Server? = null;

    @PostConstruct
    fun start() {
        log.info("Starting GRPC Server...")
        val serverBuilder = ServerBuilder.forPort(8090)
        rpcs.forEach {
            serverBuilder.addService(it)
        }

//        serverBuilder
//                .useTransportSecurity(
//                        resourceLoader.getResource("127.0.0.1.crt").inputStream,
//                        resourceLoader.getResource("127.0.0.1.p8.key").inputStream
//                )

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