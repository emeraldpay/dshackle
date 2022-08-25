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
import io.emeraldpay.api.proto.ReactorBlockchainGrpc.ReactorBlockchainStub
import io.emeraldpay.dshackle.Defaults
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.Capability
import io.emeraldpay.dshackle.upstream.ForkWatch
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.bitcoin.BitcoinUpstream
import io.emeraldpay.dshackle.upstream.bitcoin.ExtractBlock
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcGrpcClient
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.etherjar.rpc.RpcException
import io.emeraldpay.grpc.Chain
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import reactor.core.publisher.Mono
import java.math.BigInteger
import java.time.Instant
import java.util.Locale
import java.util.concurrent.TimeoutException
import java.util.function.Function

class BitcoinGrpcUpstream(
    private val parentId: String,
    forkWatch: ForkWatch,
    role: UpstreamsConfig.UpstreamRole,
    chain: Chain,
    options: UpstreamsConfig.Options,
    val remote: ReactorBlockchainGrpc.ReactorBlockchainStub,
    private val client: JsonRpcGrpcClient
) : BitcoinUpstream(
    "${parentId}_${chain.chainCode.lowercase(Locale.getDefault())}",
    chain, forkWatch,
    options,
    role
),
    GrpcUpstream,
    Lifecycle {

    companion object {
        private val log = LoggerFactory.getLogger(BitcoinGrpcUpstream::class.java)
    }

    constructor(parentId: String, role: UpstreamsConfig.UpstreamRole, chain: Chain, remote: ReactorBlockchainStub, client: JsonRpcGrpcClient) :
        this(parentId, ForkWatch.Never(), role, chain, UpstreamsConfig.Options.getDefaults(), remote, client)

    private val extractBlock = ExtractBlock()
    private val defaultReader: Reader<JsonRpcRequest, JsonRpcResponse> = client.forSelector(Selector.empty)
    private val blockConverter: Function<BlockchainOuterClass.ChainHead, BlockContainer> = Function { value ->
        val block = BlockContainer(
            value.height,
            BlockId.from(value.blockId), null,
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
        defaultReader.read(JsonRpcRequest("getblock", listOf(existingBlock.hash.toHex())))
            .flatMap(JsonRpcResponse::requireResult)
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
    private val upstreamStatus = GrpcUpstreamStatus()
    private val grpcHead = GrpcHead(chain, this, remote, blockConverter, reloadBlock)
    var timeout = Defaults.timeout
    private var capabilities: Set<Capability> = emptySet()

    override fun getBlockchainApi(): ReactorBlockchainGrpc.ReactorBlockchainStub {
        return remote
    }

    override fun getHead(): Head {
        return grpcHead
    }

    override fun getApi(): Reader<JsonRpcRequest, JsonRpcResponse> {
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
        super.start()
    }

    override fun stop() {
        super.stop()
    }

    override fun update(conf: BlockchainOuterClass.DescribeChain) {
        upstreamStatus.update(conf)
        this.capabilities = RemoteCapabilities.extract(conf)
        conf.status?.let { status -> onStatus(status) }
    }
}
