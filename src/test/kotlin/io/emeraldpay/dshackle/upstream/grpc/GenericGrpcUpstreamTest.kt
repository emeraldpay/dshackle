package io.emeraldpay.dshackle.upstream.grpc

import com.google.protobuf.ByteString
import io.emeraldpay.api.proto.BlockchainGrpc
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.config.ChainsConfig
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.test.MockGrpcServerKt
import io.emeraldpay.dshackle.upstream.finalization.FinalizationData
import io.emeraldpay.dshackle.upstream.finalization.FinalizationType
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundData
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundType
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcGrpcClient
import io.grpc.stub.StreamObserver
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import reactor.core.publisher.Sinks
import reactor.core.scheduler.Schedulers

class GenericGrpcUpstreamTest {
    private val parentId = "testParent"
    private val hash: Short = 0x01
    private val role = UpstreamsConfig.UpstreamRole.PRIMARY
    private val headSink = Sinks.many().multicast().directBestEffort<BlockchainOuterClass.ChainHead>()
    private val remote =
        MockGrpcServerKt().clientForServer(
            object : BlockchainGrpc.BlockchainImplBase() {
                override fun subscribeHead(
                    request: Common.Chain,
                    responseObserver: StreamObserver<BlockchainOuterClass.ChainHead>,
                ) {
                    Thread {
                        headSink.asFlux().subscribe { data ->
                            responseObserver.onNext(data)
                            Thread.sleep(500)
                        }
                    }.start()
                }
            },
        )
    private val client = Mockito.mock(JsonRpcGrpcClient::class.java)
    private val nodeRating = 5
    private val overrideLabels = Mockito.mock(UpstreamsConfig.Labels::class.java)
    private val headScheduler = Schedulers.single()

    private fun getUpstream(): GrpcUpstream {
        return GenericGrpcUpstream(
            parentId,
            hash,
            role,
            Chain.LINEA__MAINNET,
            remote,
            client,
            nodeRating,
            overrideLabels,
            ChainsConfig.ChainConfig.default(),
            headScheduler,
        )
    }

    @Test
    fun start() {
        val up = getUpstream()
        up.getHead().start()
        up.start()
        headSink.emitNext(
            BlockchainOuterClass.ChainHead.newBuilder()
                .setChain(Common.ChainRef.CHAIN_LINEA__MAINNET)
                .setHeight(10L)
                .setWeight(ByteString.EMPTY)
                .setBlockId("a2622ec25e883dd13c1091c18ba717a1a794713baa77b8e68ec6a993045cb50f")
                .setTimestamp(0)
                .addAllFinalizationData(
                    mutableListOf(
                        Common
                            .FinalizationData
                            .newBuilder()
                            .setType(Common.FinalizationType.FINALIZATION_FINALIZED_BLOCK)
                            .setHeight(8L)
                            .build(),
                    ),
                )
                .addAllLowerBounds(
                    mutableListOf(
                        BlockchainOuterClass.LowerBound
                            .newBuilder()
                            .setLowerBoundType(BlockchainOuterClass.LowerBoundType.LOWER_BOUND_TX)
                            .setLowerBoundTimestamp(0)
                            .setLowerBoundValue(1L).build(),
                    ),
                )
                .build(),
        ) { _, _ ->
            true
        }
        Thread.sleep(100)
        Assertions.assertEquals(1, up.getFinalizations().size)
        Assertions.assertTrue(
            up.getFinalizations()
                .contains(FinalizationData(8L, FinalizationType.FINALIZED_BLOCK)),
        )
        Assertions.assertTrue(
            up.getLowerBounds()
                .contains(LowerBoundData(1L, 0L, LowerBoundType.TX)),
        )
    }

    @Test
    fun getFinalizations() {
        val up = getUpstream()
        val finalizationData1 = FinalizationData(100L, FinalizationType.FINALIZED_BLOCK)

        val finalizationData2 = FinalizationData(200L, FinalizationType.FINALIZED_BLOCK)

        up.addFinalization(finalizationData1, "upstream1")
        up.addFinalization(finalizationData2, "upstream2")

        val finalizations = up.getFinalizations()
        Assertions.assertEquals(1, finalizations.size)
        Assertions.assertTrue(finalizations.contains(finalizationData2))
    }
}
