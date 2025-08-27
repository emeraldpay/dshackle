package io.emeraldpay.dshackle.upstream.grpc

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe

class GrpcUpstreamStatusTest :
    ShouldSpec({

        val baseConf =
            BlockchainOuterClass.DescribeChain
                .newBuilder()
                .setStatus(
                    BlockchainOuterClass.ChainStatus
                        .newBuilder()
                        .setAvailability(BlockchainOuterClass.AvailabilityEnum.AVAIL_OK)
                        .build(),
                ).addAllNodes(
                    listOf(
                        BlockchainOuterClass.NodeDetails
                            .newBuilder()
                            .build(),
                    ),
                ).setChain(Common.ChainRef.CHAIN_BITCOIN)
                .addAllSupportedMethods(listOf("foo", "bar"))
                .addAllCapabilities(listOf(BlockchainOuterClass.Capabilities.CAP_CALLS))
                .buildPartial()

        should("Not indicate update if nothing changed") {

            val state =
                GrpcUpstreamStatus().also {
                    it.update(BlockchainOuterClass.DescribeChain.newBuilder(baseConf).build())
                }

            val updated = state.update(BlockchainOuterClass.DescribeChain.newBuilder(baseConf).build())

            updated shouldBe false
        }

        should("Indicate update if methods changed") {

            val state =
                GrpcUpstreamStatus().also {
                    it.update(BlockchainOuterClass.DescribeChain.newBuilder(baseConf).build())
                }

            val updated =
                state.update(
                    BlockchainOuterClass.DescribeChain
                        .newBuilder(baseConf)
                        .addAllSupportedMethods(listOf("foo", "bar", "baz"))
                        .build(),
                )

            updated shouldBe true
        }

        should("Indicate update if labels added") {
            val state =
                GrpcUpstreamStatus().also {
                    it.update(BlockchainOuterClass.DescribeChain.newBuilder(baseConf).build())
                }

            val updated =
                state.update(
                    BlockchainOuterClass.DescribeChain
                        .newBuilder(baseConf)
                        .addAllNodes(
                            listOf(
                                BlockchainOuterClass.NodeDetails
                                    .newBuilder()
                                    .addLabels(
                                        BlockchainOuterClass.Label
                                            .newBuilder()
                                            .setName("foo")
                                            .setValue("bar"),
                                    ).build(),
                            ),
                        ).build(),
                )

            updated shouldBe true
        }
    })
