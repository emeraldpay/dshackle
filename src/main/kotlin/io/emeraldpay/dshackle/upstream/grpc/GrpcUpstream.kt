/**
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
import io.emeraldpay.dshackle.upstream.*
import io.emeraldpay.dshackle.upstream.ethereum.DefaultEthereumHead
import io.emeraldpay.dshackle.upstream.ethereum.DirectEthereumApi
import io.emeraldpay.dshackle.upstream.ethereum.EthereumHead
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.domain.BlockHash
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.*
import io.infinitape.etherjar.rpc.emerald.ReactorEmeraldClient
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.json.TransactionRefJson
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.toMono
import java.math.BigInteger
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Function
import kotlin.collections.ArrayList

open class GrpcUpstream(
        private val parentId: String,
        private val chain: Chain,
        private val blockchainStub: ReactorBlockchainGrpc.ReactorBlockchainStub,
        private val objectMapper: ObjectMapper,
        private val rpcClient: ReactorEmeraldClient
): DefaultUpstream(), Lifecycle {

    private var allLabels: Collection<UpstreamsConfig.Labels> = ArrayList<UpstreamsConfig.Labels>()
    private val log = LoggerFactory.getLogger(GrpcUpstream::class.java)

    private val options = UpstreamsConfig.Options.getDefaults()
    private val nodes = AtomicReference<NodeDetailsList>(NodeDetailsList())
    private val head = DefaultEthereumHead()
    private var targets: CallMethods? = null
    private var headSubscription: Disposable? = null

    var timeout = Defaults.timeout

    open fun createApi(matcher: Selector.Matcher): DirectEthereumApi {
        val targets = this.getMethods()
        val client = Selector.extractLabels(matcher)?.let { selector ->
            rpcClient.copyWithSelector(selector.asProto())
        } ?: rpcClient
        return DirectEthereumApi(client, objectMapper, targets).let {
            it.upstream = this
            it
        }
    }

    override fun getId(): String {
        return "$parentId/${chain.chainCode}"
    }

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
            val block = BlockJson<TransactionRefJson>()
            block.number = value.height
            block.totalDifficulty = BigInteger(1, value.weight.toByteArray())
            block.hash = BlockHash.from("0x"+value.blockId)
            block
        }.distinctUntilChanged {
            it.hash
        }.filter { block ->
            val curr = head.getCurrent()
            curr == null || curr.totalDifficulty < block.totalDifficulty
        }.flatMap {
            getApi(Selector.EmptyMatcher())
                    .flatMap { api -> api.executeAndConvert(Commands.eth().getBlock(it.hash)) }
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
        val nodes = NodeDetailsList()
        val allLabels = ArrayList<UpstreamsConfig.Labels>()
        conf.nodesList.forEach { remoteNode ->
            val node = NodeDetailsList.NodeDetails(remoteNode.quorum,
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

    fun getNodes(): NodeDetailsList {
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
        return getStatus() == UpstreamAvailability.OK && head.getCurrent() != null && nodes.get().getNodes().any {
            it.quorum > 0
        }
    }

    override fun getHead(): EthereumHead {
        return head
    }

    override fun getApi(matcher: Selector.Matcher): Mono<DirectEthereumApi> {
        return Mono.just(createApi(matcher))
    }

    override fun getOptions(): UpstreamsConfig.Options {
        return options
    }

}