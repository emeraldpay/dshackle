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
package io.emeraldpay.dshackle.rpc

import com.google.protobuf.ByteString
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.upstream.Upstreams
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.json.TransactionRefJson
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

    fun asProto(chain: Chain, block: BlockJson<TransactionRefJson>): BlockchainOuterClass.ChainHead {
        return BlockchainOuterClass.ChainHead.newBuilder()
                .setChainValue(chain.id)
                .setHeight(block.number)
                .setTimestamp(block.timestamp.toEpochMilli())
                .setWeight(ByteString.copyFrom(block.totalDifficulty.toByteArray()))
                .setBlockId(block.hash.toHex().substring(2))
                .build()
    }

}