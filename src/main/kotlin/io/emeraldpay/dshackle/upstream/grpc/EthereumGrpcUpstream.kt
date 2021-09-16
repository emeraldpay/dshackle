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
import io.emeraldpay.dshackle.Defaults
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.startup.QuorumForLabels
import io.emeraldpay.dshackle.upstream.*
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.ethereum.*
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcGrpcClient
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.grpc.Chain
import io.emeraldpay.etherjar.domain.BlockHash
import io.emeraldpay.etherjar.rpc.*
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import reactor.core.publisher.Mono
import java.math.BigInteger
import java.time.Instant
import java.util.concurrent.TimeoutException
import java.util.function.Function

open class EthereumGrpcUpstream(
        private val parentId: String,
        private val chain: Chain,
        private val remote: ReactorBlockchainGrpc.ReactorBlockchainStub,
        private val client: JsonRpcGrpcClient
) : EthereumUpstream(
        "$parentId/${chain.chainCode}",
        UpstreamsConfig.Options.getDefaults(),
        UpstreamsConfig.UpstreamRole.STANDARD,
        null, null
), GrpcUpstream, Lifecycle {

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
                    BlockContainer.fromEthereumJson(it)
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
    private val upstreamStatus = GrpcUpstreamStatus()
    private val grpcHead = GrpcHead(chain, this, blockConverter, reloadBlock)
    private var capabilities: Set<Capability> = emptySet()

    private val defaultReader: Reader<JsonRpcRequest, JsonRpcResponse> = client.forSelector(Selector.empty)
    var timeout = Defaults.timeout

    override fun start() {
        grpcHead.start(remote)
    }

    override fun isRunning(): Boolean {
        return grpcHead.isRunning
    }


    override fun stop() {
        grpcHead.stop()
    }

    override fun update(conf: BlockchainOuterClass.DescribeChain) {
        upstreamStatus.update(conf)
        capabilities = RemoteCapabilities.extract(conf)
        conf.status?.let { status -> onStatus(status) }
    }

    override fun getQuorumByLabel(): QuorumForLabels {
        return upstreamStatus.getNodes()
    }

    // ------------------------------------------------------------------------------------------

    override fun getLabels(): Collection<UpstreamsConfig.Labels> {
        return upstreamStatus.getLabels()
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

    override fun getApi(): Reader<JsonRpcRequest, JsonRpcResponse> {
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