package io.emeraldpay.dshackle.upstream.grpc

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.dshackle.upstream.Capability

class RemoteCapabilities {
    companion object {
        @JvmStatic
        fun extract(conf: BlockchainOuterClass.DescribeChain): Set<Capability> =
            conf.capabilitiesList
                ?.mapNotNull { value ->
                    when (requireNotNull(value)) {
                        BlockchainOuterClass.Capabilities.CAP_BALANCE -> Capability.BALANCE
                        BlockchainOuterClass.Capabilities.CAP_CALLS -> Capability.RPC
                        BlockchainOuterClass.Capabilities.CAP_ALLOWANCE -> Capability.ALLOWANCE
                        BlockchainOuterClass.Capabilities.CAP_NONE -> null
                        BlockchainOuterClass.Capabilities.UNRECOGNIZED -> null
                    }
                }?.toSet() ?: emptySet()
    }
}
