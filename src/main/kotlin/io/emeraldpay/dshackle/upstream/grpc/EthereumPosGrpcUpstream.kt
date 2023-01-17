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
import io.emeraldpay.dshackle.Defaults
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.reader.JsonRpcReader
import io.emeraldpay.dshackle.startup.QuorumForLabels
import io.emeraldpay.dshackle.upstream.Capability
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Lifecycle
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.ethereum.EthereumIngressSubscription
import io.emeraldpay.dshackle.upstream.ethereum.EthereumPosUpstream
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.EthereumDshackleIngressSubscription
import io.emeraldpay.dshackle.upstream.forkchoice.NoChoiceWithPriorityForkChoice
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcGrpcClient
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.etherjar.domain.BlockHash
import io.emeraldpay.etherjar.rpc.RpcException
import io.grpc.ManagedChannel
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.math.BigInteger
import java.time.Instant
import java.util.Locale
import java.util.concurrent.TimeoutException
import java.util.function.Function

open class EthereumPosGrpcUpstream(
    private val parentId: String,
    hash: Byte,
    role: UpstreamsConfig.UpstreamRole,
    private val chain: Chain,
    private val remote: ReactorBlockchainGrpc.ReactorBlockchainStub,
    client: JsonRpcGrpcClient,
    nodeRating: Int,
    overrideLabels: UpstreamsConfig.Labels?
) : EthereumPosUpstream(
    "${parentId}_${chain.chainCode.lowercase(Locale.getDefault())}",
    hash,
    UpstreamsConfig.Options.getDefaults(),
    role,
    null, null
),
    GrpcUpstream,
    Lifecycle {

    private val blockConverter: Function<BlockchainOuterClass.ChainHead, BlockContainer> = Function { value ->
        val block = BlockContainer(
            value.height,
            BlockId.from(BlockHash.from("0x" + value.blockId)),
            BigInteger(1, value.weight.toByteArray()),
            Instant.ofEpochMilli(value.timestamp),
            false,
            null,
            null
        )
        block
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

    private val log = LoggerFactory.getLogger(EthereumGrpcUpstream::class.java)
    private val upstreamStatus = GrpcUpstreamStatus(overrideLabels)
    private val grpcHead = GrpcHead(chain, this, remote, blockConverter, reloadBlock, NoChoiceWithPriorityForkChoice(nodeRating, parentId))
    private var capabilities: Set<Capability> = emptySet()

    private val defaultReader: JsonRpcReader = client.getReader()
    var timeout = Defaults.timeout
    private val ethereumSubscriptions = EthereumDshackleIngressSubscription(chain, remote)

    override fun start() {
    }

    override fun isRunning(): Boolean {
        return true
    }

    override fun stop() {

    }

    override fun update(conf: BlockchainOuterClass.DescribeChain) {
        upstreamStatus.update(conf)
        capabilities = RemoteCapabilities.extract(conf)
        conf.status?.let { status -> onStatus(status) }
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
