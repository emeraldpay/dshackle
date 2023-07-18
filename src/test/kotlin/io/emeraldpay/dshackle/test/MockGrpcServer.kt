package io.emeraldpay.dshackle.test

import io.emeraldpay.api.proto.BlockchainGrpc
import io.emeraldpay.api.proto.ReactorBlockchainGrpc
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import io.grpc.testing.GrpcCleanupRule
import org.slf4j.LoggerFactory

class MockGrpcServer {

    companion object {
        private val log = LoggerFactory.getLogger(MockGrpcServer::class.java)
    }

    val grpcCleanup = GrpcCleanupRule()

    fun clientForServer(base: BlockchainGrpc.BlockchainImplBase): ReactorBlockchainGrpc.ReactorBlockchainStub {
        val serverName = InProcessServerBuilder.generateName()
        grpcCleanup.register(
            InProcessServerBuilder
                .forName(serverName).directExecutor().addService(base).build().start()
        )
        val channel = grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build())
        return ReactorBlockchainGrpc.newReactorStub(channel)
    }
}
