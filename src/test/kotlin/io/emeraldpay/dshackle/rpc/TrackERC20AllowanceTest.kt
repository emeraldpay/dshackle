package io.emeraldpay.dshackle.rpc

import io.emeraldpay.api.Chain
import io.emeraldpay.api.proto.BlockchainOuterClass.AddressAllowance
import io.emeraldpay.api.proto.BlockchainOuterClass.AddressAllowanceRequest
import io.emeraldpay.api.proto.Common
import io.emeraldpay.api.proto.ReactorBlockchainGrpc.ReactorBlockchainStub
import io.emeraldpay.dshackle.upstream.ApiSource
import io.emeraldpay.dshackle.upstream.EmptyHead
import io.emeraldpay.dshackle.upstream.MultistreamHolder
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.ethereum.EthereumMultistream
import io.emeraldpay.dshackle.upstream.grpc.EthereumGrpcUpstream
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.justRun
import io.mockk.mockk
import io.mockk.slot
import org.reactivestreams.Subscriber
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

private const val USDT = "0xdac17f958d2ee523a2206206994597c13d831ec7" // Tether: USDT Stablecoin

class TrackERC20AllowanceTest :
    ShouldSpec({

        should("supports ethereum chain") {
            val trackERC20Allowance = trackERC20Allowance()
            val request = allowanceRequest("0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D")
            val actual = trackERC20Allowance.isSupported(request)
            actual shouldBe true
        }

        should("get address allowance") {
            val request = allowanceRequest("0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D")
            val grpcResponse = addressAllowance("0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D", "0x2a65aca4d5fc5b5c859090a6c34d164135398226")
            val trackERC20Allowance =
                trackERC20Allowance(reactorBlockchainStubInitializer = { stub ->
                    every { stub.getAddressAllowance(request) } returns Flux.just(grpcResponse)
                    stub
                })
            val actual = trackERC20Allowance.getAddressAllowance(request).blockFirst()
            actual shouldBe grpcResponse
        }

        should("subscribe address allowance") {
            val request = allowanceRequest("0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D")
            val grpcResponse = addressAllowance("0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D", "0x2a65aca4d5fc5b5c859090a6c34d164135398226")
            val trackERC20Allowance =
                trackERC20Allowance(reactorBlockchainStubInitializer = { stub ->
                    every { stub.subscribeAddressAllowance(request) } returns Flux.just(grpcResponse)
                    stub
                })
            val actual = trackERC20Allowance.subscribeAddressAllowance(request).blockFirst()
            actual shouldBe grpcResponse
        }
    })

fun addressAllowance(
    address: String,
    owner: String,
): AddressAllowance =
    AddressAllowance
        .newBuilder()
        .setChainValue(Chain.ETHEREUM.id)
        .setAddress(
            Common.SingleAddress.newBuilder().setAddress(address),
        ).setContractAddress(
            Common.SingleAddress.newBuilder().setAddress(USDT),
        ).setOwnerAddress(
            Common.SingleAddress.newBuilder().setAddress(owner),
        ).setSpenderAddress(
            Common.SingleAddress.newBuilder().setAddress(address),
        ).setAllowance("1234560000000000000000")
        .setAvailable("1234000000000000000000")
        .build()

fun allowanceRequest(address: String): AddressAllowanceRequest =
    AddressAllowanceRequest
        .newBuilder()
        .setAddress(
            Common.AnyAddress
                .newBuilder()
                .setAddressSingle(
                    Common.SingleAddress
                        .newBuilder()
                        .setAddress(address),
                ),
        ).setChainValue(Chain.ETHEREUM.id)
        .build()

private fun trackERC20Allowance(
    upstreamInitializer: (EthereumMultistream) -> EthereumMultistream = { it },
    apiSourceInitializer: (ApiSource) -> ApiSource = { it },
    reactorBlockchainStubInitializer: (ReactorBlockchainStub) -> ReactorBlockchainStub = { it },
): TrackERC20Allowance {
    var reactorBlockchainStub = mockk<ReactorBlockchainStub>()
    reactorBlockchainStub = reactorBlockchainStubInitializer(reactorBlockchainStub)

    val grpcUpstream = mockk<EthereumGrpcUpstream>()
    every { grpcUpstream.cast(EthereumGrpcUpstream::class.java) } returns grpcUpstream
    every { grpcUpstream.remote } returns reactorBlockchainStub

    var apiSource = mockk<ApiSource>()
    justRun { apiSource.request(any()) }
    val slot = slot<Subscriber<Upstream>>()
    every { apiSource.subscribe(capture(slot)) } answers {
        Mono.just(grpcUpstream).subscribe(slot.captured)
    }
    apiSource = apiSourceInitializer(apiSource)
    var upstream = mockk<EthereumMultistream>()
    every { upstream.cast(EthereumMultistream::class.java) } returns upstream
    every { upstream.getHead() } returns EmptyHead()
    every { upstream.getApiSource(any()) } returns apiSource
    upstream = upstreamInitializer(upstream)

    val holder = mockk<MultistreamHolder>()
    every { holder.isAvailable(Chain.ETHEREUM) } returns true
    every { holder.getUpstream(Chain.ETHEREUM) } returns upstream

    return TrackERC20Allowance(holder)
}
