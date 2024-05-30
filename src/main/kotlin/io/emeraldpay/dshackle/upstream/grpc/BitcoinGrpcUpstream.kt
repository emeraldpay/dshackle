/**
 * Copyright (c) 2020 EmeraldPay, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.emeraldpay.dshackle.upstream.grpc

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.ReactorBlockchainGrpc
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Defaults
import io.emeraldpay.dshackle.config.ChainsConfig
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.foundation.ChainOptions
import io.emeraldpay.dshackle.reader.ChainReader
import io.emeraldpay.dshackle.upstream.BuildInfo
import io.emeraldpay.dshackle.upstream.Capability
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.ChainResponse
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Lifecycle
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.bitcoin.BitcoinUpstream
import io.emeraldpay.dshackle.upstream.bitcoin.ExtractBlock
import io.emeraldpay.dshackle.upstream.ethereum.rpc.RpcException
import io.emeraldpay.dshackle.upstream.forkchoice.MostWorkForkChoice
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundData
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundType
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcGrpcClient
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler
import java.math.BigInteger
import java.time.Instant
import java.util.Locale
import java.util.concurrent.TimeoutException
import java.util.function.Function

class BitcoinGrpcUpstream(
    private val parentId: String,
    role: UpstreamsConfig.UpstreamRole,
    chain: Chain,
    val remote: ReactorBlockchainGrpc.ReactorBlockchainStub,
    client: JsonRpcGrpcClient,
    overrideLabels: UpstreamsConfig.Labels?,
    chainConfig: ChainsConfig.ChainConfig,
    headScheduler: Scheduler,
) : BitcoinUpstream(
    "${parentId}_${chain.chainCode.lowercase(Locale.getDefault())}",
    chain,
    ChainOptions.PartialOptions.getDefaults().buildOptions(),
    role,
    chainConfig,
),
    GrpcUpstream,
    Lifecycle {

    private val extractBlock = ExtractBlock()
    private val defaultReader: ChainReader = client.getReader()
    private val blockConverter: Function<BlockchainOuterClass.ChainHead, GrpcHead.GrpcHeadData> = Function { value ->
        val parentHash =
            if (value.parentBlockId.isBlank()) {
                null
            } else {
                BlockId.from(value.parentBlockId)
            }
        val block = BlockContainer(
            value.height,
            BlockId.from(value.blockId),
            BigInteger(1, value.weight.toByteArray()),
            Instant.ofEpochMilli(value.timestamp),
            false,
            null,
            null,
            parentHash,
        )
        GrpcHead.GrpcHeadData(block)
    }

    private val reloadBlock: Function<BlockContainer, Publisher<BlockContainer>> = Function { existingBlock ->
        // head comes without transaction data
        // need to download transactions for the block
        defaultReader.read(ChainRequest("getblock", ListParams(existingBlock.hash.toHex())))
            .flatMap(ChainResponse::requireResult)
            .map(extractBlock::extract)
            .timeout(timeout, Mono.error(TimeoutException("Timeout from upstream")))
            .doOnError { t ->
                setStatus(UpstreamAvailability.UNAVAILABLE)
                val msg = "Failed to download block data for chain $chain on $parentId"
                if (t is RpcException || t is TimeoutException) {
                    log.warn("$msg. Message: ${t.message}")
                } else {
                    log.error(msg, t)
                }
            }
    }
    private val upstreamStatus = GrpcUpstreamStatus(overrideLabels)
    private val grpcHead = GrpcHead(
        getId(),
        chain,
        this,
        remote,
        blockConverter,
        reloadBlock,
        MostWorkForkChoice(),
        headScheduler,
    )
    private val timeout = Defaults.timeout
    private var capabilities: Set<Capability> = emptySet()
    private val buildInfo: BuildInfo = BuildInfo()

    override fun getBlockchainApi(): ReactorBlockchainGrpc.ReactorBlockchainStub {
        return remote
    }

    override fun proxySubscribe(request: BlockchainOuterClass.NativeSubscribeRequest): Flux<out Any> =
        remote.nativeSubscribe(request)

    override fun getHead(): Head {
        return grpcHead
    }

    override fun getIngressReader(): ChainReader {
        return defaultReader
    }

    override fun getLabels(): Collection<UpstreamsConfig.Labels> {
        return upstreamStatus.getLabels()
    }

    override fun getCapabilities(): Set<Capability> {
        return capabilities
    }

    override fun isGrpc(): Boolean {
        return true
    }

    override fun getLowerBounds(): Collection<LowerBoundData> {
        return emptyList()
    }

    override fun getLowerBound(lowerBoundType: LowerBoundType): LowerBoundData? {
        return null
    }

    override fun getUpstreamSettingsData(): Upstream.UpstreamSettingsData? {
        return null
    }

    @Suppress("UNCHECKED_CAST")
    override fun <T : Upstream> cast(selfType: Class<T>): T {
        if (!selfType.isAssignableFrom(this.javaClass)) {
            throw ClassCastException("Cannot cast ${this.javaClass} to $selfType")
        }
        return this as T
    }

    override fun isRunning(): Boolean {
        return true
    }

    override fun start() {
    }

    override fun stop() {
    }

    override fun getBuildInfo(): BuildInfo {
        return buildInfo
    }

    override fun update(conf: BlockchainOuterClass.DescribeChain, buildInfo: BlockchainOuterClass.BuildInfo): Boolean {
        val newBuildInfo = BuildInfo.extract(buildInfo)
        val buildInfoChanged = this.buildInfo.update(newBuildInfo)
        val newCapabilities = RemoteCapabilities.extract(conf)
        val upstreamStatusChanged = (upstreamStatus.update(conf) || (newCapabilities != capabilities)).also {
            capabilities = newCapabilities
        }
        conf.status?.let { status -> onStatus(status) }
        return buildInfoChanged || upstreamStatusChanged
    }
}
