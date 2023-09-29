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
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.BlockchainOuterClass.NativeSubscribeReplyItem
import io.emeraldpay.dshackle.BlockchainType
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.SilentException
import io.emeraldpay.dshackle.upstream.MultistreamHolder
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.ethereum.EthereumLikeMultistream
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.json.HasUpstream
import io.emeraldpay.dshackle.upstream.signature.ResponseSigner
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
    @Autowired private val signer: ResponseSigner,
) {

    companion object {
        private val log = LoggerFactory.getLogger(NativeSubscribe::class.java)
    }

    private val objectMapper = Global.objectMapper

    fun nativeSubscribe(request: Mono<BlockchainOuterClass.NativeSubscribeRequest>): Flux<NativeSubscribeReplyItem> {
        return request
            .flatMapMany(this@NativeSubscribe::start)
            .map(this@NativeSubscribe::convertToProto)
            .onErrorMap(this@NativeSubscribe::convertToStatus)
    }

    fun start(request: BlockchainOuterClass.NativeSubscribeRequest): Publisher<ResponseHolder> {
        val chain = Chain.byId(request.chainValue)
        if (BlockchainType.from(chain) != BlockchainType.EVM_POS && BlockchainType.from(chain) != BlockchainType.EVM_POW) {
            return Mono.error(UnsupportedOperationException("Native subscribe is not supported for ${chain.chainCode}"))
        }

        val nonce = request.nonce.takeIf { it != 0L }
        val matcher = Selector.convertToMatcher(request.selector)

        /**
         * Try to proxy request subscription directly to the upstream dshackle instance.
         * If not possible - performs subscription logic on the current instance
         * @see EthereumLikeMultistream.tryProxy
         */
        val publisher = getUpstream(chain).tryProxy(matcher, request) ?: run {
            val method = request.method
            val params: Any? = request.payload?.takeIf { !it.isEmpty }?.let {
                objectMapper.readValue(it.newInput(), Map::class.java)
            }
            subscribe(chain, method, params, matcher)
        }
        return publisher.map { ResponseHolder(it, nonce) }
    }

    fun convertToStatus(t: Throwable) = when (t) {
        is SilentException.UnsupportedBlockchain -> StatusException(
            Status.UNAVAILABLE.withDescription("BLOCKCHAIN UNAVAILABLE: ${t.blockchainId}"),
        )

        is UnsupportedOperationException -> StatusException(
            Status.UNIMPLEMENTED.withDescription(t.message),
        )

        else -> {
            log.warn("Unhandled error", t)
            StatusException(
                Status.INTERNAL.withDescription(t.message),
            )
        }
    }

    open fun subscribe(chain: Chain, method: String, params: Any?, matcher: Selector.Matcher): Flux<out Any> =
        getUpstream(chain).getEgressSubscription()
            .subscribe(method, params, matcher)
            .doOnError {
                log.error("Error during subscription to $method, chain $chain, params $params", it)
            }

    private fun getUpstream(chain: Chain): EthereumLikeMultistream =
        multistreamHolder.getUpstream(chain).let { it as EthereumLikeMultistream }

    fun convertToProto(holder: ResponseHolder): NativeSubscribeReplyItem {
        if (holder.response is NativeSubscribeReplyItem) {
            return holder.response
        }
        val result = objectMapper.writeValueAsBytes(holder.response)
        val builder = NativeSubscribeReplyItem.newBuilder()
            .setPayload(ByteString.copyFrom(result))

        holder.nonce?.also { nonce ->
            holder.getSource()?.let {
                signer.sign(nonce, result, it)
            }?.let {
                buildSignature(nonce, it)
            }?.also {
                builder.signature = it
            }
        }

        holder.getSource()?.let { builder.setUpstreamId(it) }

        return builder.build()
    }

    fun buildSignature(
        nonce: Long,
        signature: ResponseSigner.Signature,
    ): BlockchainOuterClass.NativeCallReplySignature {
        val msg = BlockchainOuterClass.NativeCallReplySignature.newBuilder()
        msg.signature = ByteString.copyFrom(signature.value)
        msg.keyId = signature.keyId
        msg.upstreamId = signature.upstreamId
        msg.nonce = nonce
        return msg.build()
    }

    data class ResponseHolder(
        val response: Any,
        val nonce: Long?,
    ) {
        fun getSource(): String? =
            if (response is HasUpstream) {
                response.upstreamId.takeIf { it != "unknown" }
            } else {
                null
            }
    }
}
