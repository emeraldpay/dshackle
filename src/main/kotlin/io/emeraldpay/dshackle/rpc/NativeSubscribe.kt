/**
 * Copyright (c) 2021 EmeraldPay, Inc
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
import io.emeraldpay.api.Chain
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.SilentException
import io.emeraldpay.dshackle.upstream.HasEgressSubscription
import io.emeraldpay.dshackle.upstream.MultistreamHolder
import io.grpc.Status
import io.grpc.StatusException
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@Service
open class NativeSubscribe(
    @Autowired private val multistreamHolder: MultistreamHolder,
) {
    companion object {
        private val log = LoggerFactory.getLogger(NativeSubscribe::class.java)
    }

    private val objectMapper = Global.objectMapper

    fun nativeSubscribe(request: Mono<BlockchainOuterClass.NativeSubscribeRequest>): Flux<BlockchainOuterClass.NativeSubscribeReplyItem> =
        request
            .flatMapMany(this@NativeSubscribe::start)
            .map(this@NativeSubscribe::convertToProto)
            .onErrorMap(this@NativeSubscribe::convertToStatus)

    fun start(request: BlockchainOuterClass.NativeSubscribeRequest): Publisher<out Any> {
        val chain = Chain.byId(request.chainValue)
        val method = request.method
        val params: Any? =
            request.payload?.let { payload ->
                if (payload.size() > 0) {
                    objectMapper.readValue(payload.newInput(), Map::class.java)
                } else {
                    null
                }
            }
        return subscribe(chain, method, params)
    }

    fun convertToStatus(t: Throwable) =
        when (t) {
            is SilentException.UnsupportedBlockchain ->
                StatusException(
                    Status.UNAVAILABLE.withDescription("BLOCKCHAIN UNAVAILABLE: ${t.blockchainId}"),
                )
            is UnsupportedOperationException ->
                StatusException(
                    Status.UNIMPLEMENTED.withDescription(t.message),
                )
            else -> {
                log.warn("Unhandled error", t)
                StatusException(
                    Status.INTERNAL.withDescription(t.message),
                )
            }
        }

    open fun subscribe(
        chain: Chain,
        method: String,
        params: Any?,
    ): Flux<out Any> {
        val up =
            multistreamHolder.getUpstream(chain) as? HasEgressSubscription
                ?: return Flux.error(SilentException.UnsupportedBlockchain(chain))
        return up
            .getEgressSubscription()
            .subscribe(method, params)
    }

    fun convertToProto(value: Any): BlockchainOuterClass.NativeSubscribeReplyItem {
        val result =
            when (value) {
                is ByteArray -> value
                else -> objectMapper.writeValueAsBytes(value)
            }
        return BlockchainOuterClass.NativeSubscribeReplyItem
            .newBuilder()
            .setPayload(ByteString.copyFrom(result))
            .build()
    }
}
