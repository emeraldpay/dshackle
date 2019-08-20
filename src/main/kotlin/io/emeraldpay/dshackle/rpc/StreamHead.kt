package io.emeraldpay.dshackle.rpc

import com.google.protobuf.ByteString
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.upstream.Upstreams
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.json.BlockJson
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@Service
class StreamHead(
        @Autowired private val upstreams: Upstreams
) {

    private val log = LoggerFactory.getLogger(StreamHead::class.java)

    fun add(requestMono: Mono<Common.Chain>): Flux<BlockchainOuterClass.ChainHead> {
        return requestMono.map { request ->
            Chain.byId(request.type.number)
        }.flatMapMany { chain ->
            val up = upstreams.getUpstream(chain)
                    ?: return@flatMapMany Flux.error<BlockchainOuterClass.ChainHead>(Exception("Unavailable chain: $chain"))
            up.getHead()
                    .getFlux()
                    .map { asProto(chain, it) }
        }
    }

    fun asProto(chain: Chain, block: BlockJson<TransactionId>): BlockchainOuterClass.ChainHead {
        return BlockchainOuterClass.ChainHead.newBuilder()
                .setChainValue(chain.id)
                .setHeight(block.number)
                .setTimestamp(block.timestamp.time)
                .setWeight(ByteString.copyFrom(block.totalDifficulty.toByteArray()))
                .setBlockId(block.hash.toHex().substring(2))
                .build()
    }

}