package io.emeraldpay.dshackle.config.context

import brave.grpc.GrpcTracing
import brave.rpc.RpcTracing
import brave.sampler.Sampler
import io.grpc.ServerInterceptor
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
open class TraceConfiguration {

    @Bean
    open fun grpcTracing(rpcTracing: RpcTracing): GrpcTracing = GrpcTracing.create(rpcTracing)

    @Bean
    open fun grpcServerBraveInterceptor(grpcTracing: GrpcTracing): ServerInterceptor =
        grpcTracing.newServerInterceptor()

    @Bean
    open fun defaultSampler(): Sampler = Sampler.ALWAYS_SAMPLE
}
