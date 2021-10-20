package io.emeraldpay.dshackle.upstream.grpc

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.dshackle.upstream.Capability

class RemoteCapabilities {

    companion object {
        @JvmStatic
        fun extract(conf: BlockchainOuterClass.DescribeChain): Set<Capability> {
            return conf.capabilitiesList?.let { values ->
                values.mapNotNull { value ->
                    when {
                        BlockchainOuterClass.Capabilities.CAP_BALANCE == value -> Capability.BALANCE
                        BlockchainOuterClass.Capabilities.CAP_CALLS == value -> Capability.RPC
                        else -> null
                    }
                }.toSet()
            } ?: emptySet()
        }
    }
}
