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
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.upstream.*
import io.emeraldpay.dshackle.upstream.ethereum.DirectEthereumApi
import io.emeraldpay.dshackle.upstream.ethereum.EthereumHead
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.domain.BlockHash
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.*
import io.infinitape.etherjar.rpc.json.BlockJson
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.TopicProcessor
import reactor.core.publisher.toMono
import java.lang.Exception
import java.math.BigInteger
import java.time.Duration
import java.util.*
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Function

open class GrpcUpstream(
        private val chain: Chain,
        private val client: ReactorBlockchainGrpc.ReactorBlockchainStub,
        private val objectMapper: ObjectMapper,
        private val targets: CallMethods
): DefaultUpstream(), Lifecycle {

    private val log = LoggerFactory.getLogger(GrpcUpstream::class.java)

    private val options = UpstreamsConfig.Options.getDefaults()
    private val headBlock = AtomicReference<BlockJson<TransactionId>>(null)
    private val streamBlocks: TopicProcessor<BlockJson<TransactionId>> = TopicProcessor.create()
    private val nodes = AtomicReference<NodeDetailsList>(NodeDetailsList())
    private val head = Head(this)
    private val supportedMethods = HashSet<String>()
    private val grpcTransport = EthereumGrpcTransport(chain, client, objectMapper)

    private var headSubscription: Disposable? = null

    open fun createApi(matcher: Selector.Matcher): DirectEthereumApi {
        val rpcClient = DefaultRpcClient(grpcTransport.withMatcher(matcher))
        return DirectEthereumApi(rpcClient, objectMapper, targets).let {
            it.upstream = this
            it
        }
    }

    override fun start() {
        val chainRef = Common.Chain.newBuilder()
                .setTypeValue(chain.id)
                .build()
                .toMono()

        val retry: Function<Flux<BlockchainOuterClass.ChainHead>, Flux<BlockchainOuterClass.ChainHead>> = Function {
            setStatus(UpstreamAvailability.UNAVAILABLE)
            client.subscribeHead(chainRef)
        }

        val flux = client.subscribeHead(chainRef)
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
        headSubscription = flux.map { value ->
                    val block = BlockJson<TransactionId>()
                    block.number = value.height
                    block.totalDifficulty = BigInteger(1, value.weight.toByteArray())
                    block.hash = BlockHash.from("0x"+value.blockId)
                    block
                }
                .distinctUntilChanged { it.hash }
                .filter { block ->
                    val curr = headBlock.get()
                    curr == null || curr.totalDifficulty < block.totalDifficulty
                }
                .flatMap {
                    getApi(Selector.EmptyMatcher())
                            .executeAndConvert(Commands.eth().getBlock(it.hash))
                            .timeout(Duration.ofSeconds(5), Mono.error(Exception("Timeout requesting block from upstream")))
                            .doOnError { t ->
                                log.warn("Failed to download block data", t)
                            }
                }
                .onErrorContinue { err, _ ->
                    log.error("Head subscription error: ${err.message}")
                }
                .subscribe { block ->
                    log.debug("New block ${block.number} on ${chain}")
                    setStatus(UpstreamAvailability.OK)
                    headBlock.set(block)
                    streamBlocks.onNext(block)
                }
    }

    fun init(conf: BlockchainOuterClass.DescribeChain) {
        supportedMethods.addAll(conf.supportedMethodsList)
        val nodes = NodeDetailsList()
        conf.nodesList.forEach { node ->
            val node = NodeDetailsList.NodeDetails(node.quorum,
                    node.labelsList.let { provided ->
                        val labels = UpstreamsConfig.Labels()
                        provided.forEach { labels.put(it.name, it.value) }
                        labels
                    }
            )
            nodes.add(node)
        }
        this.nodes.set(nodes)
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

    override fun getSupportedTargets(): Set<String> {
        return supportedMethods
    }

    override fun isAvailable(matcher: Selector.Matcher): Boolean {
        return getStatus() == UpstreamAvailability.OK && headBlock.get() != null && nodes.get().getNodes().any {
            it.quorum > 0 && matcher.matches(it.labels)
        }
    }

    override fun getHead(): EthereumHead {
        return head
    }

    override fun getApi(matcher: Selector.Matcher): DirectEthereumApi {
        return createApi(matcher)
    }

    override fun getOptions(): UpstreamsConfig.Options {
        return options
    }

    class Head(
            val upstream: GrpcUpstream
    ): EthereumHead {

        override fun getFlux(): Flux<BlockJson<TransactionId>> {
            return Flux.merge(
                    Mono.justOrEmpty(upstream.headBlock.get()),
                    Flux.from(upstream.streamBlocks)
            )
        }
    }

}