package io.emeraldpay.dshackle.upstream.grpc

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.dshackle.upstream.Capability
import spock.lang.Specification

class RemoteCapabilitiesSpec extends Specification {

    def "parse from remote - all"() {
        setup:
        def remote = BlockchainOuterClass.DescribeChain.newBuilder()
                .addCapabilities(BlockchainOuterClass.Capabilities.CAP_CALLS)
                .addCapabilities(BlockchainOuterClass.Capabilities.CAP_NONE)
                .addCapabilities(BlockchainOuterClass.Capabilities.CAP_BALANCE)
                .addCapabilities(BlockchainOuterClass.Capabilities.CAP_ALLOWANCE)
                .build()
        when:
        def act = RemoteCapabilities.extract(remote)
        then:
        act == [Capability.BALANCE, Capability.RPC, Capability.ALLOWANCE].toSet()
    }

    def "parse from remote - only call"() {
        setup:
        def remote = BlockchainOuterClass.DescribeChain.newBuilder()
                .addCapabilities(BlockchainOuterClass.Capabilities.CAP_CALLS)
                .build()
        when:
        def act = RemoteCapabilities.extract(remote)
        then:
        act == [Capability.RPC].toSet()
    }
}
