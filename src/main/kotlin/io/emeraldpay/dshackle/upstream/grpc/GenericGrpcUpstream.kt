/**
 * Copyright (c) 2020 EmeraldPay, Inc
 * Copyright (c) 2019 ETCDEV GmbH
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
import io.emeraldpay.dshackle.config.ChainsConfig
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.foundation.ChainOptions
import io.emeraldpay.dshackle.reader.ChainReader
import io.emeraldpay.dshackle.startup.QuorumForLabels
import io.emeraldpay.dshackle.upstream.BuildInfo
import io.emeraldpay.dshackle.upstream.Capability
import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Lifecycle
import io.emeraldpay.dshackle.upstream.LowerBoundBlockDetector
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.ethereum.domain.BlockHash
import io.emeraldpay.dshackle.upstream.forkchoice.NoChoiceWithPriorityForkChoice
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcGrpcClient
import reactor.core.publisher.Flux
import reactor.core.scheduler.Scheduler
import java.math.BigInteger
import java.time.Instant
import java.util.Locale
import java.util.function.Function

open class GenericGrpcUpstream(
    parentId: String,
    hash: Byte,
    role: UpstreamsConfig.UpstreamRole,
    chain: Chain,
    private val remote: ReactorBlockchainGrpc.ReactorBlockchainStub,
    client: JsonRpcGrpcClient,
    nodeRating: Int,
    overrideLabels: UpstreamsConfig.Labels?,
    chainConfig: ChainsConfig.ChainConfig,
    headScheduler: Scheduler,
) : DefaultUpstream(
    "${parentId}_${chain.chainCode.lowercase(Locale.getDefault())}",
    hash,
    ChainOptions.PartialOptions.getDefaults().buildOptions(),
    role,
    null,
    null,
    chainConfig,
),
    GrpcUpstream,
    Lifecycle {

    private val blockConverter: Function<BlockchainOuterClass.ChainHead, BlockContainer> = Function { value ->
        val parentHash =
            if (value.parentBlockId.isBlank()) {
                null
            } else {
                BlockId.from(BlockHash.from("0x" + value.parentBlockId))
            }
        val block = BlockContainer(
            value.height,
            BlockId.from(BlockHash.from("0x" + value.blockId)),
            BigInteger(1, value.weight.toByteArray()),
            Instant.ofEpochMilli(value.timestamp),
            false,
            null,
            null,
            parentHash,
        )
        block
    }

    private val upstreamStatus = GrpcUpstreamStatus(overrideLabels)
    private val grpcHead = GrpcHead(
        getId(),
        chain,
        this,
        remote,
        blockConverter,
        null,
        NoChoiceWithPriorityForkChoice(nodeRating, parentId),
        headScheduler,
    )
    private var capabilities: Set<Capability> = emptySet()
    private val buildInfo: BuildInfo = BuildInfo()

    private val defaultReader: ChainReader = client.getReader()

    override fun start() {
    }

    override fun isRunning(): Boolean {
        return true
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

    override fun getQuorumByLabel(): QuorumForLabels {
        return upstreamStatus.getNodes()
    }

    override fun getBlockchainApi(): ReactorBlockchainGrpc.ReactorBlockchainStub {
        return remote
    }

    override fun proxySubscribe(request: BlockchainOuterClass.NativeSubscribeRequest): Flux<out Any> =
        remote.nativeSubscribe(request)

    // ------------------------------------------------------------------------------------------

    override fun getLabels(): Collection<UpstreamsConfig.Labels> {
        return upstreamStatus.getLabels()
    }

    override fun getMethods(): CallMethods {
        return upstreamStatus.getCallMethods()
    }

    override fun isAvailable(): Boolean {
        return super.isAvailable() && getQuorumByLabel().getAll().any {
            it.quorum > 0
        }
    }

    override fun getHead(): Head {
        return grpcHead
    }

    override fun getIngressReader(): ChainReader {
        return defaultReader
    }

    @Suppress("UNCHECKED_CAST")
    override fun <T : Upstream> cast(selfType: Class<T>): T {
        if (!selfType.isAssignableFrom(this.javaClass)) {
            throw ClassCastException("Cannot cast ${this.javaClass} to $selfType")
        }
        return this as T
    }

    override fun getCapabilities(): Set<Capability> {
        return capabilities
    }

    override fun isGrpc(): Boolean {
        return true
    }

    override fun getLowerBlock(): LowerBoundBlockDetector.LowerBlockData {
        return LowerBoundBlockDetector.LowerBlockData.default()
    }

    override fun getUpstreamSettingsData(): Upstream.UpstreamSettingsData? {
        return null
    }
}
