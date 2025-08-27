package io.emeraldpay.dshackle.rpc

import io.emeraldpay.api.BlockchainType
import io.emeraldpay.api.Chain
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.ReactorBlockchainGrpc
import io.emeraldpay.dshackle.Defaults
import io.emeraldpay.dshackle.SilentException
import io.emeraldpay.dshackle.upstream.Capability
import io.emeraldpay.dshackle.upstream.MultistreamHolder
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.ethereum.EthereumMultistream
import io.emeraldpay.dshackle.upstream.grpc.EthereumGrpcUpstream
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@Service
class TrackERC20Allowance(
    @Autowired private val multistreamHolder: MultistreamHolder,
) {
    companion object {
        private val log = LoggerFactory.getLogger(TrackERC20Allowance::class.java)
    }

    private val allowanceUpstreamMatcher =
        Selector.LocalAndMatcher(
            Selector.GrpcMatcher(),
            Selector.CapabilityMatcher(Capability.ALLOWANCE),
        )

    fun isSupported(request: BlockchainOuterClass.AddressAllowanceRequest): Boolean {
        val chain = Chain.byId(request.chainValue)
        if (BlockchainType.from(chain) != BlockchainType.ETHEREUM) {
            return false
        }
        return multistreamHolder.getUpstream(chain)?.getApiSource(allowanceUpstreamMatcher) != null
    }

    fun getAddressAllowance(request: BlockchainOuterClass.AddressAllowanceRequest): Flux<BlockchainOuterClass.AddressAllowance> {
        val chain = Chain.byId(request.chainValue)
        return getAllowanceGrpc(chain).flatMapMany { remote ->
            remote.getAddressAllowance(request)
        }
    }

    fun subscribeAddressAllowance(request: BlockchainOuterClass.AddressAllowanceRequest): Flux<BlockchainOuterClass.AddressAllowance> {
        val chain = Chain.byId(request.chainValue)
        return getAllowanceGrpc(chain).flatMapMany { remote ->
            remote.subscribeAddressAllowance(request)
        }
    }

    private fun getAllowanceGrpc(chain: Chain): Mono<ReactorBlockchainGrpc.ReactorBlockchainStub> {
        val upstream =
            multistreamHolder
                .getUpstream(chain)
                ?.cast(EthereumMultistream::class.java)
                ?: return Mono.error(SilentException.UnsupportedBlockchain(chain.id))
        return getAllowanceGrpc(upstream)
    }

    private fun getAllowanceGrpc(api: EthereumMultistream): Mono<ReactorBlockchainGrpc.ReactorBlockchainStub> {
        val ups = api.getApiSource(allowanceUpstreamMatcher)
        ups.request(1)
        return Mono
            .from(ups)
            .map { up ->
                up.cast(EthereumGrpcUpstream::class.java).remote
            }.timeout(Defaults.timeoutInternal, Mono.empty())
            .switchIfEmpty(
                Mono
                    .fromCallable {
                        log.warn("No upstream providing allowance for ${api.chain}")
                    }.then(Mono.error(SilentException.DataUnavailable("ALLOWANCE"))),
            )
    }
}
