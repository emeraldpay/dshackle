package io.emeraldpay.dshackle.test

import io.emeraldpay.api.proto.BlockchainGrpc
import io.emeraldpay.api.proto.ReactorBlockchainGrpc
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import io.grpc.testing.GrpcCleanupRule

class MockServer {

    GrpcCleanupRule grpcCleanup = new GrpcCleanupRule()

    ReactorBlockchainGrpc.ReactorBlockchainStub clientForServer(ReactorBlockchainGrpc.BlockchainImplBase impl){
        String serverName = InProcessServerBuilder.generateName()
        grpcCleanup.register(InProcessServerBuilder
                .forName(serverName).directExecutor().addService(impl).build().start());
        def channel =  grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build())
        return ReactorBlockchainGrpc.newReactorStub(channel)
    }

    ReactorBlockchainGrpc.ReactorBlockchainStub clientForServer(BlockchainGrpc.BlockchainImplBase impl){
        String serverName = InProcessServerBuilder.generateName()
        grpcCleanup.register(InProcessServerBuilder
                .forName(serverName).directExecutor().addService(impl).build().start());
        def channel =  grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build())
        return ReactorBlockchainGrpc.newReactorStub(channel)
    }
}
