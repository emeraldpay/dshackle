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

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.protobuf.ByteString
import io.emeraldpay.api.BlockchainType
import io.emeraldpay.api.Chain
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.SilentException
import io.emeraldpay.dshackle.monitoring.record.RequestRecord
import io.emeraldpay.dshackle.upstream.Capability
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.MultistreamHolder
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.calls.EthereumCallSelector
import io.emeraldpay.dshackle.upstream.ethereum.EthereumMultistream
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcError
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcException
import io.emeraldpay.dshackle.upstream.signature.ResponseSigner
import io.emeraldpay.etherjar.rpc.RpcException
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.util.EnumMap

@Service
open class NativeCall(
    @Autowired private val multistreamHolder: MultistreamHolder,
) {
    private val log = LoggerFactory.getLogger(NativeCall::class.java)
    private val objectMapper: ObjectMapper = Global.objectMapper
    private val ethereumCallSelectors = EnumMap<Chain, EthereumCallSelector>(Chain::class.java)

    init {
        multistreamHolder.observeChains().subscribe { chain ->
            if (BlockchainType.from(chain) == BlockchainType.ETHEREUM && !ethereumCallSelectors.containsKey(chain)) {
                multistreamHolder.getUpstream(chain)?.let { up ->
                    val caches = up.cast(EthereumMultistream::class.java).caches
                    ethereumCallSelectors[chain] = EthereumCallSelector(caches.getLastHeightByHash())
                }
            }
        }
    }

    open fun nativeCall(requestMono: Mono<BlockchainOuterClass.NativeCallRequest>): Flux<BlockchainOuterClass.NativeCallReplyItem> =
        nativeCallResult(requestMono)
            .map(this::buildResponse)
            .onErrorResume(this::processException)

    open fun nativeCallResult(requestMono: Mono<BlockchainOuterClass.NativeCallRequest>): Flux<CallResult> =
        requestMono
            .flatMapMany(this::prepareCall)
            .flatMap(::processPreparedCall)
            .contextWrite(Global.monitoring.ingress.startCall(RequestRecord.Source.REQUEST))

    fun processPreparedCall(call: CallContext<RawCallDetails>): Mono<CallResult> =
        Mono
            .fromCallable { parseParams(call) }
            .flatMap { parsed ->
                this
                    .execute(parsed)
                    .contextWrite(Global.monitoring.ingress.withBlockchain(parsed.upstream.getBlockchain()))
                    .doOnError { e -> log.warn("Error during native call: ${e.message}") }
            }

    fun parseParams(it: CallContext<RawCallDetails>): CallContext<ParsedCallDetails> {
        val params = extractParams(it.payload.params)
        return it.withPayload(ParsedCallDetails(it.payload.method, params))
    }

    fun buildResponse(it: CallResult): BlockchainOuterClass.NativeCallReplyItem {
        val result =
            BlockchainOuterClass.NativeCallReplyItem
                .newBuilder()
                .setSucceed(!it.isError())
                .setId(it.id)
        if (it.isError()) {
            it.error?.let { error ->
                result.setErrorMessage(error.message)
            }
        } else {
            result.payload = ByteString.copyFrom(it.result)
        }
        if (it.nonce != null && it.signature != null) {
            result.signature = buildSignature(it.nonce, it.signature)
        }
        return result.build()
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

    fun processException(it: Throwable?): Mono<BlockchainOuterClass.NativeCallReplyItem> =
        if (it == null) {
            Mono.just(
                BlockchainOuterClass.NativeCallReplyItem
                    .newBuilder()
                    .setSucceed(false)
                    .setErrorMessage("Internal error")
                    .build(),
            )
        } else {
            val error = CallError.from(it)
            Mono.just(
                BlockchainOuterClass.NativeCallReplyItem
                    .newBuilder()
                    .setSucceed(false)
                    .setErrorMessage(error.message)
                    .setId(error.id)
                    .build(),
            )
        }

    fun prepareCall(request: BlockchainOuterClass.NativeCallRequest): Flux<CallContext<RawCallDetails>> {
        val chain = Chain.byId(request.chain.number)
        if (chain == Chain.UNSPECIFIED) {
            return Flux.error(CallFailure(0, SilentException.UnsupportedBlockchain(request.chain.number)))
        }

        if (!multistreamHolder.isAvailable(chain)) {
            return Flux.error(CallFailure(0, SilentException.UnsupportedBlockchain(request.chain.number)))
        }

        val upstream =
            multistreamHolder.getUpstream(chain)
                ?: return Flux.error(CallFailure(0, SilentException.UnsupportedBlockchain(chain)))

        return prepareCall(request, upstream)
            .contextWrite(Global.monitoring.ingress.withBlockchain(chain))
    }

    fun prepareCall(
        request: BlockchainOuterClass.NativeCallRequest,
        upstream: Multistream,
    ): Flux<CallContext<RawCallDetails>> {
        val chain = Chain.byId(request.chainValue)
        return Flux
            .fromIterable(request.itemsList)
            .flatMap {
                prepareIndividualCall(chain, request, it, upstream)
            }
    }

    fun prepareIndividualCall(
        chain: Chain,
        request: BlockchainOuterClass.NativeCallRequest,
        requestItem: BlockchainOuterClass.NativeCallItem,
        upstream: Multistream,
    ): Mono<CallContext<RawCallDetails>> {
        val method = requestItem.method
        val params = requestItem.payload.toStringUtf8()

        // for ethereum the actual block needed for the call may be specified in the call parameters
        val callSpecificMatcher: Mono<Selector.Matcher> =
            if (BlockchainType.from(upstream.chain) == BlockchainType.ETHEREUM) {
                ethereumCallSelectors[chain]?.getMatcher(method, params, upstream.getHead())
            } else {
                null
            } ?: Mono.empty()

        return callSpecificMatcher.defaultIfEmpty(Selector.empty).map { csm ->
            val matcher =
                Selector
                    .Builder()
                    .withMatcher(Selector.CapabilityMatcher(Capability.RPC))
                    .withMatcher(csm)
                    .forMethod(method)
                    .forLabels(Selector.convertToMatcher(request.selector))

            val nonce = requestItem.nonce.let { if (it == 0L) null else it }
            CallContext(requestItem.id, nonce, upstream, matcher.build(), RawCallDetails(method, params))
        }
    }

    fun execute(ctx: CallContext<ParsedCallDetails>): Mono<CallResult> =
        ctx.upstream
            .read(
                DshackleRequest(
                    id = 1,
                    method = ctx.payload.method,
                    params = ctx.payload.params,
                    nonce = ctx.nonce,
                    matcher = ctx.matcher,
                ),
            ).map {
                if (it.error != null) {
                    CallResult.fail(ctx.id, ctx.nonce, CallError(ctx.id, it.error.message, it.error))
                } else {
                    CallResult.ok(ctx.id, ctx.nonce, it.resultOrEmpty, it.providedSignature)
                }
            }.onErrorResume { t ->
                Mono.just(CallResult.fail(ctx.id, ctx.nonce, t))
            }.switchIfEmpty(
                Mono.just(
                    CallResult.fail(
                        ctx.id,
                        ctx.nonce,
                        CallError(1, "No response or no available upstream for ${ctx.payload.method}", null),
                    ),
                ),
            )

    @Suppress("UNCHECKED_CAST")
    private fun extractParams(jsonParams: String): List<Any> {
        if (StringUtils.isEmpty(jsonParams)) {
            return emptyList()
        }
        val req = objectMapper.readValue(jsonParams, List::class.java)
        return req as List<Any>
    }

    open class CallContext<T>(
        val id: Int,
        val nonce: Long?,
        val upstream: Multistream,
        val matcher: Selector.Matcher,
        val payload: T,
    ) {
        fun <X> withPayload(payload: X): CallContext<X> = CallContext(id, nonce, upstream, matcher, payload)
    }

    data class CallFailure(
        val id: Int,
        val reason: Throwable,
    ) : Exception("Failed to call $id: ${reason.message}")

    data class CallError(
        val id: Int,
        val message: String,
        val upstreamError: JsonRpcError?,
    ) {
        companion object {
            fun from(t: Throwable): CallError =
                when (t) {
                    is JsonRpcException -> CallError(t.id.asNumber().toInt(), t.error.message, t.error)
                    is RpcException -> CallError(0, t.rpcMessage, JsonRpcError(code = t.code, message = t.rpcMessage, details = t.details))
                    is CallFailure -> from(t.reason).copy(id = t.id)
                    else -> {
                        // May only happen if it's an unhandled exception.
                        // In this case try to find a meaningless details in the stack. Most important reason for doing that is to find an ID of the request
                        if (t.cause != null) {
                            from(t.cause!!)
                        } else {
                            CallError(0, t.message ?: "Upstream Error", null)
                        }
                    }
                }
        }
    }

    open class CallResult(
        val id: Int,
        val nonce: Long?,
        val result: ByteArray?,
        val error: CallError?,
        val signature: ResponseSigner.Signature?,
    ) {
        companion object {
            fun ok(
                id: Int,
                nonce: Long?,
                result: ByteArray,
                signature: ResponseSigner.Signature?,
            ): CallResult = CallResult(id, nonce, result, null, signature)

            fun fail(
                id: Int,
                nonce: Long?,
                error: CallError,
            ): CallResult = CallResult(id, nonce, null, error, null)

            fun fail(
                id: Int,
                nonce: Long?,
                error: Throwable,
            ): CallResult = CallResult(id, nonce, null, CallError.from(error), null)
        }

        fun isError(): Boolean = error != null
    }

    class RawCallDetails(
        val method: String,
        val params: String,
    )

    class ParsedCallDetails(
        val method: String,
        val params: List<Any>,
    )
}
