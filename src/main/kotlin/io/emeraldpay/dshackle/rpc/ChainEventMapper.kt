package io.emeraldpay.dshackle.rpc

import com.google.protobuf.ByteString
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.BlockchainOuterClass.SupportedMethodsEvent
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.startup.QuorumForLabels
import io.emeraldpay.dshackle.upstream.Capability
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.finalization.FinalizationData
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundData
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundType
import org.springframework.stereotype.Component

@Component
class ChainEventMapper {

    fun mapHead(head: BlockContainer): BlockchainOuterClass.ChainEvent {
        return BlockchainOuterClass.ChainEvent.newBuilder()
            .setHead(
                BlockchainOuterClass.HeadEvent.newBuilder()
                    .setHeight(head.height)
                    .setSlot(head.slot)
                    .setTimestamp(head.timestamp.toEpochMilli())
                    .setWeight(ByteString.copyFrom(head.difficulty.toByteArray()))
                    .setBlockId(head.hash.toHex())
                    .setParentBlockId(head.parentHash?.toHex() ?: "")
                    .build(),
            )
            .build()
    }

    fun mapCapabilities(capabilities: Collection<Capability>): BlockchainOuterClass.ChainEvent {
        val caps = capabilities.map {
            when (it) {
                Capability.RPC -> BlockchainOuterClass.Capabilities.CAP_CALLS
                Capability.BALANCE -> BlockchainOuterClass.Capabilities.CAP_BALANCE
                Capability.WS_HEAD -> BlockchainOuterClass.Capabilities.CAP_WS_HEAD
            }
        }

        return BlockchainOuterClass.ChainEvent.newBuilder()
            .setCapabilitiesEvent(
                BlockchainOuterClass.CapabilitiesEvent.newBuilder()
                    .addAllCapabilities(caps)
                    .build(),
            )
            .build()
    }

    fun mapNodeDetails(nodeDetails: Collection<QuorumForLabels.QuorumItem>): BlockchainOuterClass.ChainEvent {
        val details = nodeDetails.map {
            BlockchainOuterClass.NodeDetails.newBuilder()
                .setQuorum(it.quorum)
                .addAllLabels(
                    it.labels.entries.map { label ->
                        BlockchainOuterClass.Label.newBuilder()
                            .setName(label.key)
                            .setValue(label.value)
                            .build()
                    },
                ).build()
        }

        return BlockchainOuterClass.ChainEvent.newBuilder()
            .setNodesEvent(
                BlockchainOuterClass.NodeDetailsEvent.newBuilder()
                    .addAllNodes(details)
                    .build(),
            )
            .build()
    }

    fun mapFinalizationData(finalizationData: Collection<FinalizationData>): BlockchainOuterClass.ChainEvent {
        val data = finalizationData.map {
            Common.FinalizationData.newBuilder()
                .setHeight(it.height)
                .setType(it.type.toProtoFinalizationType())
                .build()
        }

        return BlockchainOuterClass.ChainEvent.newBuilder()
            .setFinalizationDataEvent(
                BlockchainOuterClass.FinalizationDataEvent.newBuilder()
                    .addAllFinalizationData(data)
                    .build(),
            )
            .build()
    }

    fun mapLowerBounds(lowerBounds: Collection<LowerBoundData>): BlockchainOuterClass.ChainEvent {
        val data = lowerBounds
            .map {
                BlockchainOuterClass.LowerBound.newBuilder()
                    .setLowerBoundTimestamp(it.timestamp)
                    .setLowerBoundType(mapLowerBoundType(it.type))
                    .setLowerBoundValue(it.lowerBound)
                    .build()
            }

        return BlockchainOuterClass.ChainEvent.newBuilder()
            .setLowerBoundsEvent(
                BlockchainOuterClass.LowerBoundEvent.newBuilder()
                    .addAllLowerBounds(data)
                    .build(),
            )
            .build()
    }

    fun chainStatus(status: UpstreamAvailability): BlockchainOuterClass.ChainEvent {
        return BlockchainOuterClass.ChainEvent.newBuilder()
            .setStatus(
                BlockchainOuterClass.ChainStatus.newBuilder()
                    .setAvailability(Common.AvailabilityEnum.forNumber(status.grpcId))
                    .build(),
            )
            .build()
    }

    fun supportedMethods(methods: Collection<String>): BlockchainOuterClass.ChainEvent {
        return BlockchainOuterClass.ChainEvent.newBuilder()
            .setSupportedMethodsEvent(
                SupportedMethodsEvent.newBuilder()
                    .addAllMethods(methods)
                    .build(),
            )
            .build()
    }

    fun supportedSubs(subs: Collection<String>): BlockchainOuterClass.ChainEvent {
        return BlockchainOuterClass.ChainEvent.newBuilder()
            .setSupportedSubscriptionsEvent(
                BlockchainOuterClass.SupportedSubscriptionsEvent.newBuilder()
                    .addAllSubs(subs)
                    .build(),
            )
            .build()
    }

    private fun mapLowerBoundType(type: LowerBoundType): BlockchainOuterClass.LowerBoundType {
        return when (type) {
            LowerBoundType.SLOT -> BlockchainOuterClass.LowerBoundType.LOWER_BOUND_SLOT
            LowerBoundType.UNKNOWN -> BlockchainOuterClass.LowerBoundType.LOWER_BOUND_UNSPECIFIED
            LowerBoundType.STATE -> BlockchainOuterClass.LowerBoundType.LOWER_BOUND_STATE
            LowerBoundType.BLOCK -> BlockchainOuterClass.LowerBoundType.LOWER_BOUND_BLOCK
            LowerBoundType.TX -> BlockchainOuterClass.LowerBoundType.LOWER_BOUND_TX
            LowerBoundType.LOGS -> BlockchainOuterClass.LowerBoundType.LOWER_BOUND_LOGS
            LowerBoundType.TRACE -> BlockchainOuterClass.LowerBoundType.LOWER_BOUND_TRACE
            LowerBoundType.PROOF -> BlockchainOuterClass.LowerBoundType.LOWER_BOUND_PROOF
            LowerBoundType.BLOB -> BlockchainOuterClass.LowerBoundType.LOWER_BOUND_BLOB
            LowerBoundType.EPOCH -> BlockchainOuterClass.LowerBoundType.LOWER_BOUND_EPOCH
        }
    }
}
