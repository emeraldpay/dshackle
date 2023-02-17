package io.emeraldpay.dshackle.config.context

import brave.grpc.GrpcTracing
import brave.rpc.RpcTracing
import io.grpc.ServerInterceptor
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
open class TraceGrpcConfiguration {

    @Bean
    open fun grpcTracing(rpcTracing: RpcTracing): GrpcTracing = GrpcTracing.create(rpcTracing)

    @Bean
    open fun grpcServerBraveInterceptor(grpcTracing: GrpcTracing): ServerInterceptor =
        grpcTracing.newServerInterceptor()
}
