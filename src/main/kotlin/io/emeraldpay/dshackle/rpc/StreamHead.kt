package io.emeraldpay.dshackle.rpc

import com.google.protobuf.ByteString
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.upstream.AvailableChains
import io.emeraldpay.dshackle.upstream.Upstreams
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.json.BlockJson
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.TopicProcessor
import reactor.core.publisher.toFlux
import java.lang.Exception
import java.util.concurrent.ConcurrentLinkedQueue
import javax.annotation.PostConstruct
import kotlin.collections.HashMap

@Service
class StreamHead(
        @Autowired private val upstreams: Upstreams,
        @Autowired private val availableChains: AvailableChains
) {

    private val log = LoggerFactory.getLogger(StreamHead::class.java)
    private val clients = HashMap<Chain, ConcurrentLinkedQueue<TopicProcessor<BlockchainOuterClass.ChainHead>>>()

    @PostConstruct
    fun init() {
        availableChains.observe().subscribe { chain ->
            clients[chain] = ConcurrentLinkedQueue()
            subscribe(chain)
        }
    }

    private fun subscribe(chain: Chain) {
        upstreams.getUpstream(chain)?.let { up ->
            up.getHead()
                .getFlux()
                .doOnComplete {
                    log.info("Closing streams for ${chain.chainCode}")
                    clients.replace(chain, ConcurrentLinkedQueue())!!.forEach { client ->
                        try {
                            client.dispose()
                        } catch (e: Throwable) {
                        }
                    }
                }
                .subscribe { block -> onBlock(chain, block) }
        }
    }

    private fun onBlock(chain: Chain, block: BlockJson<TransactionId>) {
        log.info("New block ${block.number} on ${chain.chainCode}")
        clients[chain]!!.toFlux()
                .subscribe { stream ->
                    notify(chain, block, stream)
                }
    }

    fun add(requestMono: Mono<Common.Chain>): Flux<BlockchainOuterClass.ChainHead> {
        return requestMono.map { request ->
            Chain.byId(request.type.number)
        }.filter {
            it != Chain.UNSPECIFIED && clients.containsKey(it)
        }.flatMapMany { chain ->
            val sender = TopicProcessor.create<BlockchainOuterClass.ChainHead>()
            clients[chain]!!.add(sender)
            notify(chain, sender)
            sender
        }
    }

    fun notify(chain: Chain, client: TopicProcessor<BlockchainOuterClass.ChainHead>) {
        val upstream = upstreams.getUpstream(chain) ?: return
        val head = upstream.getHead().getHead()
        head.subscribe {
            notify(chain, it, client)
        }
    }

    fun notify(chain: Chain, block: BlockJson<TransactionId>, client: TopicProcessor<BlockchainOuterClass.ChainHead>) {
        val data = BlockchainOuterClass.ChainHead.newBuilder()
                .setChainValue(chain.id)
                .setHeight(block.number)
                .setTimestamp(block.timestamp.time)
                .setWeight(ByteString.copyFrom(block.totalDifficulty.toByteArray()))
                .setBlockId(block.hash.toHex().substring(2))
                .build()
        client.onNext(data)
    }


}