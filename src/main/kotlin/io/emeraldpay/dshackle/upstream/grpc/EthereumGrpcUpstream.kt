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

import com.fasterxml.jackson.databind.ObjectMapper
import com.salesforce.reactorgrpc.GrpcRetry
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.api.proto.ReactorBlockchainGrpc
import io.emeraldpay.dshackle.Defaults
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.startup.QuorumForLabels
import io.emeraldpay.dshackle.upstream.*
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.calls.DirectCallMethods
import io.emeraldpay.dshackle.upstream.ethereum.*
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcGrpcClient
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.domain.BlockHash
import io.infinitape.etherjar.rpc.*
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.toMono
import java.math.BigInteger
import java.time.Duration
import java.time.Instant
import java.util.*
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Function
import kotlin.collections.ArrayList

open class EthereumGrpcUpstream(
        private val parentId: String,
        private val chain: Chain,
        private val blockchainStub: ReactorBlockchainGrpc.ReactorBlockchainStub,
        private val client: JsonRpcGrpcClient
) : DefaultUpstream(
        "$parentId/${chain.chainCode}",
        UpstreamsConfig.Options.getDefaults(),
        UpstreamsConfig.UpstreamRole.STANDARD,
        null
), Lifecycle {

    private var allLabels: Collection<UpstreamsConfig.Labels> = ArrayList<UpstreamsConfig.Labels>()
    private val log = LoggerFactory.getLogger(EthereumGrpcUpstream::class.java)

    private val nodes = AtomicReference<QuorumForLabels>(QuorumForLabels())
    private val head = DefaultEthereumHead()
    private var targets: CallMethods? = null
    private var headSubscription: Disposable? = null

    var timeout = Defaults.timeout

    private val defaultReader: Reader<JsonRpcRequest, JsonRpcResponse> = client.forSelector(Selector.empty)

    override fun start() {
        if (this.isRunning) return
        val chainRef = Common.Chain.newBuilder()
                .setTypeValue(chain.id)
                .build()
                .toMono()

        val retry: Function<Flux<BlockchainOuterClass.ChainHead>, Flux<BlockchainOuterClass.ChainHead>> = Function {
            setStatus(UpstreamAvailability.UNAVAILABLE)
            blockchainStub.subscribeHead(chainRef)
        }

        val flux = blockchainStub.subscribeHead(chainRef)
                .compose(GrpcRetry.ManyToMany.retryAfter(retry, Duration.ofSeconds(5)))
        observeHead(flux)
    }

    override fun isRunning(): Boolean {
        return headSubscription != null
    }


    override fun stop() {
        headSubscription?.dispose()
        headSubscription = null
    }


    internal fun observeHead(flux: Flux<BlockchainOuterClass.ChainHead>) {
        val base = flux.map { value ->
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
        }.distinctUntilChanged {
            it.hash
        }.filter { block ->
            val curr = head.getCurrent()
            curr == null || curr.difficulty < block.difficulty
        }.flatMap {
            defaultReader.read(JsonRpcRequest("eth_getBlockByHash", listOf(it.hash.toHexWithPrefix(), false)))
                    .flatMap(JsonRpcResponse::requireResult)
                    .map {
                        BlockContainer.from(it)
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
        }.onErrorContinue { err, _ ->
            log.error("Head subscription error. ${err.javaClass.name}:${err.message}", err)
        }.doOnNext {
            setStatus(UpstreamAvailability.OK)
        }

        headSubscription = head.follow(base)
    }

    fun init(conf: BlockchainOuterClass.DescribeChain) {
        targets = DirectCallMethods(conf.supportedMethodsList.toSet())
        val nodes = QuorumForLabels()
        val allLabels = ArrayList<UpstreamsConfig.Labels>()
        conf.nodesList.forEach { remoteNode ->
            val node = QuorumForLabels.QuorumItem(remoteNode.quorum,
                    remoteNode.labelsList.let { provided ->
                        val labels = UpstreamsConfig.Labels()
                        provided.forEach {
                            labels[it.name] = it.value
                        }
                        allLabels.add(labels)
                        labels
                    }
            )
            nodes.add(node)
        }
        this.nodes.set(nodes)
        this.allLabels = Collections.unmodifiableCollection(allLabels)
        conf.status?.let { status -> onStatus(status) }
    }

    fun onStatus(value: BlockchainOuterClass.ChainStatus) {
        val available = value.availability
        val quorum = value.quorum
        setStatus(
                if (available != null) UpstreamAvailability.fromGrpc(available.number) else UpstreamAvailability.UNAVAILABLE
        )
    }

    fun getNodes(): QuorumForLabels {
        return nodes.get()
    }

    // ------------------------------------------------------------------------------------------

    override fun getLabels(): Collection<UpstreamsConfig.Labels> {
        return allLabels
    }

    override fun getMethods(): CallMethods {
        return targets ?: throw IllegalStateException("Upstream is not initialized yet")
    }

    override fun isAvailable(): Boolean {
        return super.isAvailable() && head.getCurrent() != null && nodes.get().getAll().any {
            it.quorum > 0
        }
    }

    override fun getHead(): Head {
        return head
    }

    override fun getApi(): Reader<JsonRpcRequest, JsonRpcResponse> {
        return defaultReader
    }

    @SuppressWarnings("unchecked")
    override fun <T : Upstream> cast(selfType: Class<T>): T {
        if (!selfType.isAssignableFrom(this.javaClass)) {
            throw ClassCastException("Cannot cast ${this.javaClass} to $selfType")
        }
        return this as T
    }

}