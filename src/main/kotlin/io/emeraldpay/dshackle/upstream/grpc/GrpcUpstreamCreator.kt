package io.emeraldpay.dshackle.upstream.grpc

import brave.grpc.GrpcTracing
import io.emeraldpay.dshackle.Defaults
import io.emeraldpay.dshackle.FileResolver
import io.emeraldpay.dshackle.config.AuthorizationConfig
import io.emeraldpay.dshackle.config.ChainsConfig
import io.emeraldpay.dshackle.config.CompressionConfig
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.startup.configure.UpstreamCreator.Companion.getHash
import io.emeraldpay.dshackle.upstream.grpc.auth.GrpcAuthContext
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers
import java.util.concurrent.Executor
import java.util.concurrent.Executors

@Component
class GrpcUpstreamCreator(
    private val authorizationConfig: AuthorizationConfig,
    private val compressionConfig: CompressionConfig,
    private val fileResolver: FileResolver,
    @Qualifier("grpcChannelExecutor")
    private val channelExecutor: Executor,
    private val grpcTracing: GrpcTracing,
    @Qualifier("headScheduler")
    private val headScheduler: Scheduler,
    private val grpcAuthContext: GrpcAuthContext,
) {
    @Value("\${spring.application.max-metadata-size}")
    private var maxMetadataSize: Int = Defaults.maxMetadataSize

    private val hashes: MutableMap<Byte, Boolean> = HashMap()

    companion object {
        val grpcUpstreamsScheduler: Scheduler = Schedulers.fromExecutorService(
            Executors.newFixedThreadPool(2),
            "GrpcUpstreamsStatuses",
        )
    }

    fun creatGrpcUpstream(
        config: UpstreamsConfig.Upstream<UpstreamsConfig.GrpcConnection>,
        chainsConfig: ChainsConfig,
    ): GrpcUpstreams {
        val endpoint = config.connection!!
        return GrpcUpstreams(
            config.id!!,
            getHash(config.nodeId, "${endpoint.host}:${endpoint.port}", hashes),
            config.role,
            endpoint.host!!,
            endpoint.port,
            endpoint.auth,
            endpoint.tokenAuth,
            authorizationConfig,
            compressionConfig.grpc.clientEnabled,
            fileResolver,
            endpoint.upstreamRating,
            config.labels,
            grpcUpstreamsScheduler,
            channelExecutor,
            chainsConfig,
            grpcTracing,
            null,
            maxMetadataSize,
            headScheduler,
            grpcAuthContext,
        )
    }
}
