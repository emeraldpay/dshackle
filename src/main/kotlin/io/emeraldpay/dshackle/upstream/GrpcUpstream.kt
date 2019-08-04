package io.emeraldpay.dshackle.upstream

import com.fasterxml.jackson.databind.ObjectMapper
import com.salesforce.reactorgrpc.GrpcRetry
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.api.proto.ReactorBlockchainGrpc
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.domain.BlockHash
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.*
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.json.BlockTag
import org.slf4j.LoggerFactory
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
        private val options: UpstreamsConfig.Options,
        private val targets: EthereumTargets
): Upstream {

    constructor(chain: Chain, client: ReactorBlockchainGrpc.ReactorBlockchainStub, objectMapper: ObjectMapper, targets: EthereumTargets)
            : this(chain, client, objectMapper, UpstreamsConfig.Options.getDefaults(), targets)

    private val log = LoggerFactory.getLogger(GrpcUpstream::class.java)

    private val headBlock = AtomicReference<BlockJson<TransactionId>>(null)
    private val streamBlocks: TopicProcessor<BlockJson<TransactionId>> = TopicProcessor.create()
    private val status = AtomicReference<UpstreamAvailability>(UpstreamAvailability.UNAVAILABLE)
    private val nodes = AtomicReference<NodeDetailsList>(NodeDetailsList())
    private val head = Head(this)
    private val statusStream: TopicProcessor<UpstreamAvailability> = TopicProcessor.create()
    private val supportedMethods = HashSet<String>()
    private val grpcTransport = EthereumGrpcTransport(chain, client, objectMapper)

    open fun createApi(matcher: Selector.Matcher): EthereumApi {
        val rpcClient = DefaultRpcClient(grpcTransport.withMatcher(matcher))
        return EthereumApi(rpcClient, objectMapper, chain, targets, this)
    }

    open fun connect() {
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
        subscribe(flux)
    }

    internal fun subscribe(flux: Flux<BlockchainOuterClass.ChainHead>) {
        flux.map { value ->
                    val block = BlockJson<TransactionId>()
                    block.number = value.height
                    block.totalDifficulty = BigInteger(1, value.weight.toByteArray())
                    block.hash = BlockHash.from("0x"+value.blockId)
                    block
                }
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
        supportedMethods.addAll(conf.supportedTargetsList)
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

    private fun setStatus(value: UpstreamAvailability) {
        status.set(value)
        statusStream.onNext(value)
    }

    fun getNodes(): NodeDetailsList {
        return nodes.get()
    }

    // ------------------------------------------------------------------------------------------

    override fun getSupportedTargets(): Set<String> {
        return supportedMethods
    }

    override fun isAvailable(matcher: Selector.Matcher): Boolean {
        return headBlock.get() != null && nodes.get().getNodes().any {
            it.quorum > 0 && matcher.matches(it.labels)
        }
    }

    override fun getStatus(): UpstreamAvailability {
        return status.get()
    }

    override fun observeStatus(): Flux<UpstreamAvailability> {
        return Flux.from(statusStream)
    }

    override fun getHead(): EthereumHead {
        return head
    }

    override fun getApi(matcher: Selector.Matcher): EthereumApi {
        return createApi(matcher)
    }

    override fun getOptions(): UpstreamsConfig.Options {
        return options
    }

    class Head(
            val upstream: GrpcUpstream
    ): EthereumHead {

        override fun getHead(): Mono<BlockJson<TransactionId>> {
            val current = upstream.headBlock.get()
            if (current != null) {
                return Mono.just(current)
            }
            return Mono.from(upstream.streamBlocks)
        }

        override fun getFlux(): Flux<BlockJson<TransactionId>> {
            return Flux.from(upstream.streamBlocks)
        }
    }

}