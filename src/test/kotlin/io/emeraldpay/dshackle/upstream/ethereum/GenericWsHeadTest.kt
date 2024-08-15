package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.foundation.ChainOptions
import io.emeraldpay.dshackle.reader.ChainReader
import io.emeraldpay.dshackle.upstream.BlockValidator
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.ChainResponse
import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.dshackle.upstream.ethereum.domain.BlockHash
import io.emeraldpay.dshackle.upstream.ethereum.json.BlockJson
import io.emeraldpay.dshackle.upstream.ethereum.json.TransactionRefJson
import io.emeraldpay.dshackle.upstream.forkchoice.AlwaysForkChoice
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcWsClient
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import org.junit.jupiter.api.Test
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.core.scheduler.Schedulers
import reactor.test.StepVerifier
import java.math.BigInteger
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.atomic.AtomicReference

class GenericWsHeadTest {

    @Test
    fun `validate chain settings and then head sub`() {
        val block = block(10000, BlockHash.from("0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200"))
        val reader = mock<ChainReader> {
            on { read(ChainRequest("eth_getBlockByNumber", ListParams("latest", false))) } doReturn Mono.empty()
        }
        val wsSub = mock<WsSubscriptions> {
            on { connectionInfoFlux() } doReturn Flux.empty()
            on { subscribe(ChainRequest("eth_subscribe", ListParams("newHeads"))) } doReturn
                WsSubscriptions.SubscribeData(Flux.just(Global.objectMapper.writeValueAsBytes(block)), "id", AtomicReference("subId"))
        }
        val connection = mock<WsConnection> {
            on { isConnected } doReturn true
            on { callRpc(ChainRequest("eth_chainId", ListParams())) } doReturn Mono.just(ChainResponse("\"0x1\"".toByteArray(), null))
            on { callRpc(ChainRequest("net_version", ListParams())) } doReturn Mono.just(ChainResponse("\"1\"".toByteArray(), null))
        }
        val wsPool = mock<WsConnectionPool> {
            on { getConnection() } doReturn connection
        }
        val wsClient = JsonRpcWsClient(wsPool)
        val upstream = mock<DefaultUpstream> {
            on { getId() } doReturn "id"
            on { getChain() } doReturn Chain.ETHEREUM__MAINNET
            on { getOptions() } doReturn ChainOptions.PartialOptions().buildOptions()
        }

        val wsHead = GenericWsHead(
            AlwaysForkChoice(),
            BlockValidator.ALWAYS_VALID,
            reader,
            wsSub,
            Schedulers.boundedElastic(),
            Schedulers.boundedElastic(),
            upstream,
            EthereumChainSpecific,
            wsClient,
            Duration.ofSeconds(60),
        )

        StepVerifier.create(wsHead.getFlux())
            .then { wsHead.start() }
            .expectNext(BlockContainer.from(block))
            .thenCancel()
            .verify(Duration.ofSeconds(1))

        verify(connection).callRpc(ChainRequest("eth_chainId", ListParams()))
        verify(connection).callRpc(ChainRequest("net_version", ListParams()))
        verify(wsSub).subscribe(ChainRequest("eth_subscribe", ListParams("newHeads")))
    }

    @Test
    fun `validate chain settings and then fatal error`() {
        val reader = mock<ChainReader> {
            on { read(ChainRequest("eth_getBlockByNumber", ListParams("latest", false))) } doReturn Mono.empty()
        }
        val wsSub = mock<WsSubscriptions> {
            on { connectionInfoFlux() } doReturn Flux.empty()
        }
        val connection = mock<WsConnection> {
            on { isConnected } doReturn true
            on { callRpc(ChainRequest("eth_chainId", ListParams())) } doReturn Mono.just(ChainResponse("\"0x1\"".toByteArray(), null))
            on { callRpc(ChainRequest("net_version", ListParams())) } doReturn Mono.just(ChainResponse("\"155\"".toByteArray(), null))
        }
        val wsPool = mock<WsConnectionPool> {
            on { getConnection() } doReturn connection
        }
        val wsClient = JsonRpcWsClient(wsPool)
        val upstream = mock<DefaultUpstream> {
            on { getId() } doReturn "id"
            on { getChain() } doReturn Chain.ETHEREUM__MAINNET
            on { getOptions() } doReturn ChainOptions.PartialOptions().buildOptions()
        }

        val wsHead = GenericWsHead(
            AlwaysForkChoice(),
            BlockValidator.ALWAYS_VALID,
            reader,
            wsSub,
            Schedulers.boundedElastic(),
            Schedulers.boundedElastic(),
            upstream,
            EthereumChainSpecific,
            wsClient,
            Duration.ofSeconds(60),
        )

        StepVerifier.create(wsHead.headLiveness())
            .then { wsHead.start() }
            .expectNext(HeadLivenessState.FATAL_ERROR)
            .thenCancel()
            .verify(Duration.ofSeconds(3))

        verify(connection).callRpc(ChainRequest("eth_chainId", ListParams()))
        verify(connection).callRpc(ChainRequest("net_version", ListParams()))
        verify(wsSub, never()).subscribe(ChainRequest("eth_subscribe", ListParams("newHeads")))
    }

    @Test
    fun `no validate chain settings if it's disabled`() {
        val block1 = block(10000, BlockHash.from("0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200"))
        val block2 = block(25000, BlockHash.from("0x2ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200"))
        val reader = mock<ChainReader> {
            on { read(ChainRequest("eth_getBlockByNumber", ListParams("latest", false))) } doReturn Mono.empty()
        }
        val wsSub = mock<WsSubscriptions> {
            on { connectionInfoFlux() } doReturn Flux.empty()
            on { subscribe(ChainRequest("eth_subscribe", ListParams("newHeads"))) } doReturn
                WsSubscriptions.SubscribeData(Flux.just(Global.objectMapper.writeValueAsBytes(block1), Global.objectMapper.writeValueAsBytes(block2)), "id", AtomicReference("subId"))
        }
        val connection = mock<WsConnection>()
        val wsPool = mock<WsConnectionPool> {
            on { getConnection() } doReturn connection
        }
        val wsClient = JsonRpcWsClient(wsPool)
        val upstream = mock<DefaultUpstream> {
            on { getId() } doReturn "id"
            on { getChain() } doReturn Chain.ETHEREUM__MAINNET
            on { getOptions() } doReturn ChainOptions.PartialOptions(disableUpstreamValidation = true).buildOptions()
        }

        val wsHead = GenericWsHead(
            AlwaysForkChoice(),
            BlockValidator.ALWAYS_VALID,
            reader,
            wsSub,
            Schedulers.boundedElastic(),
            Schedulers.boundedElastic(),
            upstream,
            EthereumChainSpecific,
            wsClient,
            Duration.ofSeconds(60),
        )

        StepVerifier.create(wsHead.getFlux())
            .then { wsHead.start() }
            .expectNext(BlockContainer.from(block1))
            .expectNext(BlockContainer.from(block2))
            .thenCancel()
            .verify(Duration.ofSeconds(3))

        verify(connection, never()).callRpc(ChainRequest("eth_chainId", ListParams()))
        verify(connection, never()).callRpc(ChainRequest("net_version", ListParams()))
        verify(wsSub).subscribe(ChainRequest("eth_subscribe", ListParams("newHeads")))
    }

    @Test
    fun `validate chain settings, getting an error and then head sub`() {
        val block = block(10000, BlockHash.from("0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200"))
        val reader = mock<ChainReader> {
            on { read(ChainRequest("eth_getBlockByNumber", ListParams("latest", false))) } doReturn Mono.empty()
        }
        val connectionInfoSink = Sinks.many().multicast().directBestEffort<WsConnection.ConnectionInfo>()
        val wsSub = mock<WsSubscriptions> {
            on { connectionInfoFlux() } doReturn connectionInfoSink.asFlux()
            on { subscribe(ChainRequest("eth_subscribe", ListParams("newHeads"))) } doReturn
                WsSubscriptions.SubscribeData(Flux.just(Global.objectMapper.writeValueAsBytes(block)), "id", AtomicReference("subId"))
        }
        val connection = mock<WsConnection> {
            on { isConnected } doReturn true
            on { callRpc(ChainRequest("eth_chainId", ListParams())) } doReturn Mono.error(RuntimeException("err")) doReturn Mono.just(ChainResponse("\"0x1\"".toByteArray(), null))
            on { callRpc(ChainRequest("net_version", ListParams())) } doReturn Mono.just(ChainResponse("\"1\"".toByteArray(), null))
        }
        val wsPool = mock<WsConnectionPool> {
            on { getConnection() } doReturn connection
        }
        val wsClient = JsonRpcWsClient(wsPool)
        val upstream = mock<DefaultUpstream> {
            on { getId() } doReturn "id"
            on { getChain() } doReturn Chain.ETHEREUM__MAINNET
            on { getOptions() } doReturn ChainOptions.PartialOptions().buildOptions()
        }

        val wsHead = GenericWsHead(
            AlwaysForkChoice(),
            BlockValidator.ALWAYS_VALID,
            reader,
            wsSub,
            Schedulers.boundedElastic(),
            Schedulers.boundedElastic(),
            upstream,
            EthereumChainSpecific,
            wsClient,
            Duration.ofSeconds(60),
        )

        StepVerifier.create(wsHead.getFlux())
            .then {
                wsHead.start()
                connectionInfoSink.tryEmitNext(WsConnection.ConnectionInfo("id", WsConnection.ConnectionState.CONNECTED))
            }
            .expectNoEvent(Duration.ofMillis(1500))
            .then {
                wsHead.start()
            }
            .expectNext(BlockContainer.from(block))
            .thenCancel()
            .verify(Duration.ofSeconds(3))

        verify(connection, times(2)).callRpc(ChainRequest("eth_chainId", ListParams()))
        verify(connection, times(2)).callRpc(ChainRequest("net_version", ListParams()))
        verify(wsSub).subscribe(ChainRequest("eth_subscribe", ListParams("newHeads")))
    }

    @Test
    fun `emit chain heads and no suspicious head`() {
        val block1 = block(10000, BlockHash.from("0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200"))
        val block2 = block(15000, BlockHash.from("0x2ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200"))
        val reader = mock<ChainReader> {
            on { read(ChainRequest("eth_getBlockByNumber", ListParams("latest", false))) } doReturn Mono.empty()
        }
        val wsSub = mock<WsSubscriptions> {
            on { connectionInfoFlux() } doReturn Flux.empty()
            on { subscribe(ChainRequest("eth_subscribe", ListParams("newHeads"))) } doReturn
                WsSubscriptions.SubscribeData(Flux.just(Global.objectMapper.writeValueAsBytes(block1), Global.objectMapper.writeValueAsBytes(block2)), "id", AtomicReference("subId"))
        }
        val connection = mock<WsConnection> {
            on { isConnected } doReturn true
            on { callRpc(ChainRequest("eth_chainId", ListParams())) } doReturn Mono.just(ChainResponse("\"0x1\"".toByteArray(), null))
            on { callRpc(ChainRequest("net_version", ListParams())) } doReturn Mono.just(ChainResponse("\"1\"".toByteArray(), null))
        }
        val wsPool = mock<WsConnectionPool> {
            on { getConnection() } doReturn connection
        }
        val wsClient = JsonRpcWsClient(wsPool)
        val upstream = mock<DefaultUpstream> {
            on { getId() } doReturn "id"
            on { getChain() } doReturn Chain.ETHEREUM__MAINNET
            on { getOptions() } doReturn ChainOptions.PartialOptions().buildOptions()
        }

        val wsHead = GenericWsHead(
            AlwaysForkChoice(),
            BlockValidator.ALWAYS_VALID,
            reader,
            wsSub,
            Schedulers.boundedElastic(),
            Schedulers.boundedElastic(),
            upstream,
            EthereumChainSpecific,
            wsClient,
            Duration.ofSeconds(60),
        )

        StepVerifier.create(wsHead.getFlux())
            .then { wsHead.start() }
            .expectNext(BlockContainer.from(block1))
            .expectNext(BlockContainer.from(block2))
            .thenCancel()
            .verify(Duration.ofSeconds(1))

        verify(connection).callRpc(ChainRequest("eth_chainId", ListParams()))
        verify(connection).callRpc(ChainRequest("net_version", ListParams()))
        verify(wsSub).subscribe(ChainRequest("eth_subscribe", ListParams("newHeads")))
    }

    @Test
    fun `emit chain heads and validate a suspicious one`() {
        val block1 = block(10000, BlockHash.from("0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200"))
        val block2 = block(25000, BlockHash.from("0x2ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200"))
        val reader = mock<ChainReader> {
            on { read(ChainRequest("eth_getBlockByNumber", ListParams("latest", false))) } doReturn Mono.empty()
        }
        val wsSub = mock<WsSubscriptions> {
            on { connectionInfoFlux() } doReturn Flux.empty()
            on { subscribe(ChainRequest("eth_subscribe", ListParams("newHeads"))) } doReturn
                WsSubscriptions.SubscribeData(Flux.just(Global.objectMapper.writeValueAsBytes(block1), Global.objectMapper.writeValueAsBytes(block2)), "id", AtomicReference("subId"))
        }
        val connection = mock<WsConnection> {
            on { isConnected } doReturn true
            on { callRpc(ChainRequest("eth_chainId", ListParams())) } doReturn Mono.just(ChainResponse("\"0x1\"".toByteArray(), null))
            on { callRpc(ChainRequest("net_version", ListParams())) } doReturn Mono.just(ChainResponse("\"1\"".toByteArray(), null))
        }
        val wsPool = mock<WsConnectionPool> {
            on { getConnection() } doReturn connection
        }
        val wsClient = JsonRpcWsClient(wsPool)
        val upstream = mock<DefaultUpstream> {
            on { getId() } doReturn "id"
            on { getChain() } doReturn Chain.ETHEREUM__MAINNET
            on { getOptions() } doReturn ChainOptions.PartialOptions().buildOptions()
        }

        val wsHead = GenericWsHead(
            AlwaysForkChoice(),
            BlockValidator.ALWAYS_VALID,
            reader,
            wsSub,
            Schedulers.boundedElastic(),
            Schedulers.boundedElastic(),
            upstream,
            EthereumChainSpecific,
            wsClient,
            Duration.ofSeconds(60),
        )

        StepVerifier.create(wsHead.getFlux())
            .then { wsHead.start() }
            .expectNext(BlockContainer.from(block1))
            .expectNext(BlockContainer.from(block2))
            .thenCancel()
            .verify(Duration.ofSeconds(1))

        verify(connection, times(2)).callRpc(ChainRequest("eth_chainId", ListParams()))
        verify(connection, times(2)).callRpc(ChainRequest("net_version", ListParams()))
        verify(wsSub).subscribe(ChainRequest("eth_subscribe", ListParams("newHeads")))
    }

    @Test
    fun `emit chain heads, validate a suspicious one and skip it`() {
        val block1 = block(10000, BlockHash.from("0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200"))
        val block2 = block(25000, BlockHash.from("0x2ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200"))
        val reader = mock<ChainReader> {
            on { read(ChainRequest("eth_getBlockByNumber", ListParams("latest", false))) } doReturn Mono.empty()
        }
        val wsSub = mock<WsSubscriptions> {
            on { connectionInfoFlux() } doReturn Flux.empty()
            on { subscribe(ChainRequest("eth_subscribe", ListParams("newHeads"))) } doReturn
                WsSubscriptions.SubscribeData(Flux.just(Global.objectMapper.writeValueAsBytes(block1), Global.objectMapper.writeValueAsBytes(block2)), "id", AtomicReference("subId"))
        }
        val connection = mock<WsConnection> {
            on { isConnected } doReturn true
            on { callRpc(ChainRequest("eth_chainId", ListParams())) } doReturn Mono.just(ChainResponse("\"0x1\"".toByteArray(), null))
            on { callRpc(ChainRequest("net_version", ListParams())) } doReturn
                Mono.just(ChainResponse("\"1\"".toByteArray(), null)) doReturn
                Mono.just(ChainResponse("\"dsff\"".toByteArray(), null))
        }
        val wsPool = mock<WsConnectionPool> {
            on { getConnection() } doReturn connection
        }
        val wsClient = JsonRpcWsClient(wsPool)
        val upstream = mock<DefaultUpstream> {
            on { getId() } doReturn "id"
            on { getChain() } doReturn Chain.ETHEREUM__MAINNET
            on { getOptions() } doReturn ChainOptions.PartialOptions().buildOptions()
        }

        val wsHead = GenericWsHead(
            AlwaysForkChoice(),
            BlockValidator.ALWAYS_VALID,
            reader,
            wsSub,
            Schedulers.boundedElastic(),
            Schedulers.boundedElastic(),
            upstream,
            EthereumChainSpecific,
            wsClient,
            Duration.ofSeconds(60),
        )

        StepVerifier.create(wsHead.getFlux())
            .then { wsHead.start() }
            .expectNext(BlockContainer.from(block1))
            .expectNoEvent(Duration.ofMillis(500))
            .thenCancel()
            .verify(Duration.ofSeconds(1))

        verify(connection, times(2)).callRpc(ChainRequest("eth_chainId", ListParams()))
        verify(connection, times(2)).callRpc(ChainRequest("net_version", ListParams()))
        verify(wsSub).subscribe(ChainRequest("eth_subscribe", ListParams("newHeads")))
    }

    @Test
    fun `emit chain heads, validate a suspicious one and then fatal error`() {
        val block1 = block(10000, BlockHash.from("0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200"))
        val block2 = block(25000, BlockHash.from("0x2ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200"))
        val reader = mock<ChainReader> {
            on { read(ChainRequest("eth_getBlockByNumber", ListParams("latest", false))) } doReturn Mono.empty()
        }
        val wsSub = mock<WsSubscriptions> {
            on { connectionInfoFlux() } doReturn Flux.empty()
            on { subscribe(ChainRequest("eth_subscribe", ListParams("newHeads"))) } doReturn
                WsSubscriptions.SubscribeData(Flux.just(Global.objectMapper.writeValueAsBytes(block1), Global.objectMapper.writeValueAsBytes(block2)), "id", AtomicReference("subId"))
        }
        val connection = mock<WsConnection> {
            on { isConnected } doReturn true
            on { callRpc(ChainRequest("eth_chainId", ListParams())) } doReturn Mono.just(ChainResponse("\"0x1\"".toByteArray(), null))
            on { callRpc(ChainRequest("net_version", ListParams())) } doReturn
                Mono.just(ChainResponse("\"1\"".toByteArray(), null)) doReturn
                Mono.just(ChainResponse("\"5\"".toByteArray(), null))
        }
        val wsPool = mock<WsConnectionPool> {
            on { getConnection() } doReturn connection
        }
        val wsClient = JsonRpcWsClient(wsPool)
        val upstream = mock<DefaultUpstream> {
            on { getId() } doReturn "id"
            on { getChain() } doReturn Chain.ETHEREUM__MAINNET
            on { getOptions() } doReturn ChainOptions.PartialOptions().buildOptions()
        }

        val wsHead = GenericWsHead(
            AlwaysForkChoice(),
            BlockValidator.ALWAYS_VALID,
            reader,
            wsSub,
            Schedulers.boundedElastic(),
            Schedulers.boundedElastic(),
            upstream,
            EthereumChainSpecific,
            wsClient,
            Duration.ofSeconds(60),
        )

        StepVerifier.create(wsHead.getFlux())
            .then { wsHead.start() }
            .expectNext(BlockContainer.from(block1))
            .then {
                StepVerifier.create(wsHead.headLiveness())
                    .expectNext(HeadLivenessState.FATAL_ERROR)
                    .thenCancel()
                    .verify(Duration.ofSeconds(1))
            }
            .thenCancel()
            .verify(Duration.ofSeconds(1))

        verify(connection, times(2)).callRpc(ChainRequest("eth_chainId", ListParams()))
        verify(connection, times(2)).callRpc(ChainRequest("net_version", ListParams()))
        verify(wsSub).subscribe(ChainRequest("eth_subscribe", ListParams("newHeads")))
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
