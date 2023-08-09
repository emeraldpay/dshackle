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
import io.emeraldpay.api.proto.ReactorBlockchainGrpc.ReactorBlockchainStub
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Defaults
import io.emeraldpay.dshackle.config.ChainsConfig
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.reader.JsonRpcReader
import io.emeraldpay.dshackle.startup.QuorumForLabels
import io.emeraldpay.dshackle.upstream.BuildInfo
import io.emeraldpay.dshackle.upstream.Capability
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Lifecycle
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.ethereum.EthereumIngressSubscription
import io.emeraldpay.dshackle.upstream.ethereum.EthereumLikeUpstream
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.EthereumDshackleIngressSubscription
import io.emeraldpay.dshackle.upstream.forkchoice.MostWorkForkChoice
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcGrpcClient
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.etherjar.domain.BlockHash
import io.emeraldpay.etherjar.rpc.RpcException
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler
import java.math.BigInteger
import java.time.Instant
import java.util.Locale
import java.util.concurrent.TimeoutException
import java.util.function.Function

open class EthereumGrpcUpstream(
    private val parentId: String,
    hash: Byte,
    role: UpstreamsConfig.UpstreamRole,
    private val chain: Chain,
    private val remote: ReactorBlockchainStub,
    client: JsonRpcGrpcClient,
    overrideLabels: UpstreamsConfig.Labels?,
    chainConfig: ChainsConfig.ChainConfig,
    headScheduler: Scheduler,
) : EthereumLikeUpstream(
    "${parentId}_${chain.chainCode.lowercase(Locale.getDefault())}",
    hash,
    UpstreamsConfig.PartialOptions.getDefaults().buildOptions(),
    role,
    null,
    null,
    chainConfig
),
    GrpcUpstream,
    Lifecycle {

    private val blockConverter: Function<BlockchainOuterClass.ChainHead, BlockContainer> = Function { value ->
        val parentHash =
            if (value.parentBlockId.isBlank()) null
            else BlockId.from(BlockHash.from("0x" + value.parentBlockId))
        val block = BlockContainer(
            value.height,
            BlockId.from(BlockHash.from("0x" + value.blockId)),
            BigInteger(1, value.weight.toByteArray()),
            Instant.ofEpochMilli(value.timestamp),
            false,
            null,
            null,
            parentHash
        )
        block
    }

    override fun getSubscriptionTopics(): List<String> {
        return subscriptionTopics
    }

    private val reloadBlock: Function<BlockContainer, Publisher<BlockContainer>> = Function { existingBlock ->
        // head comes without transaction data
        // need to download transactions for the block
        defaultReader.read(JsonRpcRequest("eth_getBlockByHash", listOf(existingBlock.hash.toHexWithPrefix(), false)))
            .flatMap(JsonRpcResponse::requireResult)
            .map {
                BlockContainer.fromEthereumJson(it, getId())
            }
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
        getId(), chain, this, remote,
        blockConverter, reloadBlock, MostWorkForkChoice(), headScheduler
    )
    private var capabilities: Set<Capability> = emptySet()
    private val buildInfo: BuildInfo = BuildInfo()
    private var subscriptionTopics = listOf<String>()

    private val defaultReader: JsonRpcReader = client.getReader()
    var timeout = Defaults.timeout
    private val ethereumSubscriptions = EthereumDshackleIngressSubscription(chain, remote)

    override fun getBlockchainApi(): ReactorBlockchainStub {
        return remote
    }

    override fun proxySubscribe(request: BlockchainOuterClass.NativeSubscribeRequest): Flux<out Any> =
        remote.nativeSubscribe(request)

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
        conf.status?.let { status -> onStatus(status, upstreamStatusChanged) }
        val subsChanged = (conf.supportedSubscriptionsList != subscriptionTopics).also {
            subscriptionTopics = conf.supportedSubscriptionsList
        }
        return buildInfoChanged || upstreamStatusChanged || subsChanged
    }

    override fun getQuorumByLabel(): QuorumForLabels {
        return upstreamStatus.getNodes()
    }

    // ------------------------------------------------------------------------------------------

    override fun getLabels(): Collection<UpstreamsConfig.Labels> {
        return upstreamStatus.getLabels()
    }

    override fun getIngressSubscription(): EthereumIngressSubscription {
        return ethereumSubscriptions
    }

    override fun getMethods(): CallMethods {
        return upstreamStatus.getCallMethods()
    }

    override fun isAvailable(): Boolean {
        return super.isAvailable() && grpcHead.getCurrent() != null && getQuorumByLabel().getAll().any {
            it.quorum > 0
        }
    }

    override fun getHead(): Head {
        return grpcHead
    }

    override fun getIngressReader(): JsonRpcReader {
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
}
