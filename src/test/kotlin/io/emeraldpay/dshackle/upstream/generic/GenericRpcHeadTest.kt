package io.emeraldpay.dshackle.upstream.generic

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.foundation.ChainOptions
import io.emeraldpay.dshackle.reader.ChainReader
import io.emeraldpay.dshackle.upstream.BlockValidator
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.ChainResponse
import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.dshackle.upstream.ethereum.EthereumChainSpecific
import io.emeraldpay.dshackle.upstream.ethereum.HeadLivenessState
import io.emeraldpay.dshackle.upstream.ethereum.domain.BlockHash
import io.emeraldpay.dshackle.upstream.ethereum.json.BlockJson
import io.emeraldpay.dshackle.upstream.ethereum.json.TransactionRefJson
import io.emeraldpay.dshackle.upstream.forkchoice.AlwaysForkChoice
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import org.junit.jupiter.api.Test
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import reactor.test.StepVerifier
import java.math.BigInteger
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit

class GenericRpcHeadTest {

    @Test
    fun `emit chain heads and no suspicious head`() {
        val block1 = block(10000, BlockHash.from("0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200"))
        val block2 = block(10001, BlockHash.from("0x3ec1ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200"))
        val upstream = mock<DefaultUpstream> {
            on { getChain() } doReturn Chain.ETHEREUM__MAINNET
            on { getOptions() } doReturn ChainOptions.PartialOptions().buildOptions()
        }
        val reader = mock<ChainReader> {
            on { read(ChainRequest("eth_getBlockByNumber", ListParams("latest", false))) } doReturn
                Mono.just(ChainResponse(Global.objectMapper.writeValueAsBytes(block1), null)) doReturn
                Mono.just(ChainResponse(Global.objectMapper.writeValueAsBytes(block2), null))
        }
        val head = GenericRpcHead(
            reader,
            AlwaysForkChoice(),
            "id",
            BlockValidator.ALWAYS_VALID,
            upstream,
            Schedulers.boundedElastic(),
            EthereumChainSpecific,
            Duration.ofSeconds(5),
        )

        StepVerifier.withVirtualTime { head.getFlux() }
            .expectSubscription()
            .then { head.start() }
            .expectNoEvent(Duration.ofSeconds(5))
            .expectNext(BlockContainer.from(block1))
            .expectNoEvent(Duration.ofSeconds(5))
            .expectNext(BlockContainer.from(block2))
            .thenCancel()
            .verify(Duration.ofSeconds(3))

        verify(reader, times(2)).read(ChainRequest("eth_getBlockByNumber", ListParams("latest", false)))
        verify(reader, never()).read(ChainRequest("eth_chainId", ListParams()))
        verify(reader, never()).read(ChainRequest("net_version", ListParams()))
    }

    @Test
    fun `emit chain heads and no validation if it's disabled`() {
        val block1 = block(10000, BlockHash.from("0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200"))
        val block2 = block(50000, BlockHash.from("0x3ec1ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200"))
        val upstream = mock<DefaultUpstream> {
            on { getChain() } doReturn Chain.ETHEREUM__MAINNET
            on { getOptions() } doReturn ChainOptions.PartialOptions(disableUpstreamValidation = true).buildOptions()
        }
        val reader = mock<ChainReader> {
            on { read(ChainRequest("eth_getBlockByNumber", ListParams("latest", false))) } doReturn
                Mono.just(ChainResponse(Global.objectMapper.writeValueAsBytes(block1), null)) doReturn
                Mono.just(ChainResponse(Global.objectMapper.writeValueAsBytes(block2), null))
        }
        val head = GenericRpcHead(
            reader,
            AlwaysForkChoice(),
            "id",
            BlockValidator.ALWAYS_VALID,
            upstream,
            Schedulers.boundedElastic(),
            EthereumChainSpecific,
            Duration.ofSeconds(5),
        )

        StepVerifier.withVirtualTime { head.getFlux() }
            .expectSubscription()
            .then { head.start() }
            .expectNoEvent(Duration.ofSeconds(5))
            .expectNext(BlockContainer.from(block1))
            .expectNoEvent(Duration.ofSeconds(5))
            .expectNext(BlockContainer.from(block2))
            .thenCancel()
            .verify(Duration.ofSeconds(3))

        verify(reader, times(2)).read(ChainRequest("eth_getBlockByNumber", ListParams("latest", false)))
        verify(reader, never()).read(ChainRequest("eth_chainId", ListParams()))
        verify(reader, never()).read(ChainRequest("net_version", ListParams()))
    }

    @Test
    fun `emit chain heads and validate chain settings`() {
        val block1 = block(10000, BlockHash.from("0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200"))
        val block2 = block(50000, BlockHash.from("0x3ec1ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200"))
        val reader = mock<ChainReader> {
            on { read(ChainRequest("eth_chainId", ListParams())) } doReturn Mono.just(ChainResponse("\"0x1\"".toByteArray(), null))
            on { read(ChainRequest("net_version", ListParams())) } doReturn Mono.just(ChainResponse("\"1\"".toByteArray(), null))
            on { read(ChainRequest("eth_getBlockByNumber", ListParams("latest", false))) } doReturn
                Mono.just(ChainResponse(Global.objectMapper.writeValueAsBytes(block1), null)) doReturn
                Mono.just(ChainResponse(Global.objectMapper.writeValueAsBytes(block2), null))
        }
        val upstream = mock<DefaultUpstream> {
            on { getIngressReader() } doReturn reader
            on { getChain() } doReturn Chain.ETHEREUM__MAINNET
            on { getOptions() } doReturn ChainOptions.PartialOptions().buildOptions()
        }
        val head = GenericRpcHead(
            reader,
            AlwaysForkChoice(),
            "id",
            BlockValidator.ALWAYS_VALID,
            upstream,
            Schedulers.boundedElastic(),
            EthereumChainSpecific,
            Duration.ofSeconds(5),
        )

        StepVerifier.withVirtualTime { head.getFlux() }
            .expectSubscription()
            .then { head.start() }
            .expectNoEvent(Duration.ofSeconds(5))
            .expectNext(BlockContainer.from(block1))
            .expectNoEvent(Duration.ofSeconds(5))
            .expectNext(BlockContainer.from(block2))
            .thenCancel()
            .verify(Duration.ofSeconds(3))

        verify(reader, times(2)).read(ChainRequest("eth_getBlockByNumber", ListParams("latest", false)))
        verify(reader).read(ChainRequest("eth_chainId", ListParams()))
        verify(reader).read(ChainRequest("net_version", ListParams()))
    }

    @Test
    fun `emit chain heads, validate chain settings and then fatal error`() {
        val block1 = block(10000, BlockHash.from("0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200"))
        val block2 = block(50000, BlockHash.from("0x3ec1ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200"))
        val reader = mock<ChainReader> {
            on { read(ChainRequest("eth_chainId", ListParams())) } doReturn Mono.just(ChainResponse("\"0x1\"".toByteArray(), null))
            on { read(ChainRequest("net_version", ListParams())) } doReturn Mono.just(ChainResponse("\"5\"".toByteArray(), null))
            on { read(ChainRequest("eth_getBlockByNumber", ListParams("latest", false))) } doReturn
                Mono.just(ChainResponse(Global.objectMapper.writeValueAsBytes(block1), null)) doReturn
                Mono.just(ChainResponse(Global.objectMapper.writeValueAsBytes(block2), null))
        }
        val upstream = mock<DefaultUpstream> {
            on { getIngressReader() } doReturn reader
            on { getChain() } doReturn Chain.ETHEREUM__MAINNET
            on { getOptions() } doReturn ChainOptions.PartialOptions().buildOptions()
        }
        val head = GenericRpcHead(
            reader,
            AlwaysForkChoice(),
            "id",
            BlockValidator.ALWAYS_VALID,
            upstream,
            Schedulers.boundedElastic(),
            EthereumChainSpecific,
            Duration.ofSeconds(5),
        )

        StepVerifier.withVirtualTime { head.getFlux() }
            .expectSubscription()
            .then { head.start() }
            .expectNoEvent(Duration.ofSeconds(5))
            .expectNext(BlockContainer.from(block1))
            .expectNoEvent(Duration.ofSeconds(5))
            .then {
                StepVerifier.create(head.headLiveness())
                    .expectNext(HeadLivenessState.FATAL_ERROR)
                    .thenCancel()
                    .verify(Duration.ofSeconds(3))
            }
            .thenCancel()
            .verify(Duration.ofSeconds(3))

        verify(reader, times(2)).read(ChainRequest("eth_getBlockByNumber", ListParams("latest", false)))
        verify(reader).read(ChainRequest("eth_chainId", ListParams()))
        verify(reader).read(ChainRequest("net_version", ListParams()))
    }

    @Test
    fun `emit chain heads, validate chain settings and skip a received head due to error`() {
        val block1 = block(10000, BlockHash.from("0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200"))
        val block2 = block(50000, BlockHash.from("0x3ec1ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200"))
        val reader = mock<ChainReader> {
            on { read(ChainRequest("eth_chainId", ListParams())) } doReturn Mono.just(ChainResponse("sds".toByteArray(), null))
            on { read(ChainRequest("net_version", ListParams())) } doReturn Mono.just(ChainResponse("\"sad\"".toByteArray(), null))
            on { read(ChainRequest("eth_getBlockByNumber", ListParams("latest", false))) } doReturn
                Mono.just(ChainResponse(Global.objectMapper.writeValueAsBytes(block1), null)) doReturn
                Mono.just(ChainResponse(Global.objectMapper.writeValueAsBytes(block2), null))
        }
        val upstream = mock<DefaultUpstream> {
            on { getIngressReader() } doReturn reader
            on { getChain() } doReturn Chain.ETHEREUM__MAINNET
            on { getOptions() } doReturn ChainOptions.PartialOptions().buildOptions()
        }
        val head = GenericRpcHead(
            reader,
            AlwaysForkChoice(),
            "id",
            BlockValidator.ALWAYS_VALID,
            upstream,
            Schedulers.boundedElastic(),
            EthereumChainSpecific,
            Duration.ofSeconds(5),
        )

        StepVerifier.withVirtualTime { head.getFlux() }
            .expectSubscription()
            .then { head.start() }
            .expectNoEvent(Duration.ofSeconds(5))
            .expectNext(BlockContainer.from(block1))
            .expectNoEvent(Duration.ofSeconds(5))
            .thenCancel()
            .verify(Duration.ofSeconds(3))

        Thread.sleep(100)
        verify(reader, times(2)).read(ChainRequest("eth_getBlockByNumber", ListParams("latest", false)))
        verify(reader).read(ChainRequest("eth_chainId", ListParams()))
        verify(reader).read(ChainRequest("net_version", ListParams()))
    }

    private fun block(
        height: Long,
        blockHash: BlockHash,
    ) =
        BlockJson<TransactionRefJson>()
            .apply {
                number = height
                uncles = emptyList()
                totalDifficulty = BigInteger.ONE
                parentHash = blockHash
                timestamp = Instant.now().truncatedTo(ChronoUnit.SECONDS)
                hash = blockHash
            }
}
