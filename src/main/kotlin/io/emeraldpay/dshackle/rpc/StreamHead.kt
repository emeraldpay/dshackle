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
package io.emeraldpay.dshackle.rpc

import com.google.protobuf.ByteString
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.MultistreamHolder
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundData
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundType
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@Service
class StreamHead(
    @Autowired private val multistreamHolder: MultistreamHolder,
) {

    private val log = LoggerFactory.getLogger(StreamHead::class.java)

    fun add(requestMono: Mono<Common.Chain>): Flux<BlockchainOuterClass.ChainHead> {
        return requestMono.map { request ->
            Chain.byId(request.type.number)
        }.flatMapMany { chain ->
            val ms = multistreamHolder.getUpstream(chain)
            ms.getHead()
                .getFlux()
                .map { asProto(ms, chain, it!!) }
                .onErrorContinue { t, _ ->
                    log.warn("Head subscription error", t)
                }
        }
    }

    fun asProto(ms: Multistream, chain: Chain, block: BlockContainer): BlockchainOuterClass.ChainHead {
        val msLowerBounds = ms.getLowerBounds()
        val lowerBoundsProto = msLowerBounds
            .map {
                BlockchainOuterClass.LowerBound.newBuilder()
                    .setLowerBoundTimestamp(it.timestamp)
                    .setLowerBoundType(toProtoLowerBoundType(it.type))
                    .setLowerBoundValue(it.lowerBound)
                    .build()
            }
        val toOldApi = toOldApi(msLowerBounds)

        return BlockchainOuterClass.ChainHead.newBuilder()
            .setChainValue(chain.id)
            .setHeight(block.height)
            .setSlot(block.slot)
            .setCurrentLowerBlock(toOldApi.block)
            .setCurrentLowerSlot(toOldApi.slot)
            .setCurrentLowerDataTimestamp(toOldApi.timestamp)
            .addAllLowerBounds(lowerBoundsProto)
            .setTimestamp(block.timestamp.toEpochMilli())
            .setWeight(ByteString.copyFrom(block.difficulty.toByteArray()))
            .setBlockId(block.hash.toHex())
            .setParentBlockId(block.parentHash?.toHex() ?: "")
            .build()
    }

    private fun toOldApi(lowerBounds: Collection<LowerBoundData>): LowerBoundDataOldApiCompatibility {
        val lowerBlockData = lowerBounds.find { it.type == LowerBoundType.STATE } ?: LowerBoundData.default()
        val slot = lowerBounds.find { it.type == LowerBoundType.SLOT }?.lowerBound ?: 0

        return LowerBoundDataOldApiCompatibility(
            lowerBlockData.lowerBound,
            slot,
            lowerBlockData.timestamp,
        )
    }

    private fun toProtoLowerBoundType(type: LowerBoundType): BlockchainOuterClass.LowerBoundType {
        return when (type) {
            LowerBoundType.SLOT -> BlockchainOuterClass.LowerBoundType.LOWER_BOUND_SLOT
            LowerBoundType.UNKNOWN -> BlockchainOuterClass.LowerBoundType.UNRECOGNIZED
            LowerBoundType.STATE -> BlockchainOuterClass.LowerBoundType.LOWER_BOUND_STATE
        }
    }

    private data class LowerBoundDataOldApiCompatibility(
        val block: Long,
        val slot: Long,
        val timestamp: Long,
    )
}
