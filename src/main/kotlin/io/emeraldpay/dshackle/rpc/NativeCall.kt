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
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.SilentException
import io.emeraldpay.dshackle.quorum.CallQuorum
import io.emeraldpay.dshackle.quorum.NotLaggingQuorum
import io.emeraldpay.dshackle.quorum.QuorumReaderFactory
import io.emeraldpay.dshackle.upstream.ApiSource
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.MultistreamHolder
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.calls.EthereumCallSelector
import io.emeraldpay.dshackle.upstream.ethereum.EthereumMultistream
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcError
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcException
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.dshackle.upstream.signature.ResponseSigner
import io.emeraldpay.etherjar.rpc.RpcException
import io.emeraldpay.etherjar.rpc.RpcResponseError
import io.emeraldpay.grpc.BlockchainType
import io.emeraldpay.grpc.Chain
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toMono
import java.util.EnumMap

@Service
open class NativeCall(
    @Autowired private val multistreamHolder: MultistreamHolder,
    @Autowired private val signer: ResponseSigner,
) {

    private val log = LoggerFactory.getLogger(NativeCall::class.java)
    private val objectMapper: ObjectMapper = Global.objectMapper

    var quorumReaderFactory: QuorumReaderFactory = QuorumReaderFactory.default()
    private val ethereumCallSelectors = EnumMap<Chain, EthereumCallSelector>(Chain::class.java)

    init {
        multistreamHolder.observeChains().subscribe { chain ->
            if (BlockchainType.from(chain) == BlockchainType.ETHEREUM && !ethereumCallSelectors.containsKey(chain)) {
                multistreamHolder.getUpstream(chain)?.let { up ->
                    val reader = up.cast(EthereumMultistream::class.java).getReader()
                    ethereumCallSelectors[chain] = EthereumCallSelector(reader.heightByHash())
                }
            }
        }
    }

    open fun nativeCall(requestMono: Mono<BlockchainOuterClass.NativeCallRequest>): Flux<BlockchainOuterClass.NativeCallReplyItem> {
        return nativeCallResult(requestMono)
            .map(this::buildResponse)
            .onErrorResume(this::processException)
    }

    open fun nativeCallResult(requestMono: Mono<BlockchainOuterClass.NativeCallRequest>): Flux<CallResult> {
        return requestMono.flatMapMany(this::prepareCall)
            .flatMap {
                if (it.isValid()) {
                    val parsed = parseParams(it.get())
                    this.fetch(parsed)
                        .doOnError { e -> log.warn("Error during native call: ${e.message}") }
                } else {
                    val error = it.getError()
                    Mono.just(
                        CallResult(error.id, 0, null, error, null)
                    )
                }
            }
    }

    fun parseParams(it: ValidCallContext<RawCallDetails>): ValidCallContext<ParsedCallDetails> {
        val params = extractParams(it.payload.params)
        return it.withPayload(ParsedCallDetails(it.payload.method, params))
    }

    fun buildResponse(it: CallResult): BlockchainOuterClass.NativeCallReplyItem {
        val result = BlockchainOuterClass.NativeCallReplyItem.newBuilder()
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

    fun buildSignature(nonce: Long, signature: ResponseSigner.Signature): BlockchainOuterClass.NativeCallReplySignature {
        val msg = BlockchainOuterClass.NativeCallReplySignature.newBuilder()
        msg.signature = ByteString.copyFrom(signature.value)
        msg.keyId = signature.keyId
        msg.upstreamId = signature.upstreamId
        msg.nonce = nonce
        return msg.build()
    }

    fun processException(it: Throwable?): Mono<BlockchainOuterClass.NativeCallReplyItem> {
        val id: Int = if (it != null && it is CallFailure) {
            it.id
        } else {
            log.error("Lost context for a native call", it)
            0
        }
        return BlockchainOuterClass.NativeCallReplyItem.newBuilder()
            .setSucceed(false)
            .setErrorMessage(it?.message ?: "Internal error")
            .setId(id)
            .build()
            .toMono()
    }

    fun prepareCall(request: BlockchainOuterClass.NativeCallRequest): Flux<CallContext> {
        val chain = Chain.byId(request.chain.number)
        if (chain == Chain.UNSPECIFIED) {
            return Flux.error(CallFailure(0, SilentException.UnsupportedBlockchain(request.chain.number)))
        }

        if (!multistreamHolder.isAvailable(chain)) {
            return Flux.error(CallFailure(0, SilentException.UnsupportedBlockchain(request.chain.number)))
        }

        val upstream = multistreamHolder.getUpstream(chain)
            ?: return Flux.error(CallFailure(0, SilentException.UnsupportedBlockchain(chain)))

        return prepareCall(request, upstream)
    }

    fun prepareCall(
        request: BlockchainOuterClass.NativeCallRequest,
        upstream: Multistream
    ): Flux<CallContext> {
        val chain = Chain.byId(request.chainValue)
        return Flux.fromIterable(request.itemsList)
            .flatMap {
                prepareIndividualCall(chain, request, it, upstream)
            }
    }

    fun prepareIndividualCall(
        chain: Chain,
        request: BlockchainOuterClass.NativeCallRequest,
        requestItem: BlockchainOuterClass.NativeCallItem,
        upstream: Multistream
    ): Mono<CallContext> {
        val method = requestItem.method
        val params = requestItem.payload.toStringUtf8()
        val availableMethods = upstream.getMethods()

        if (!availableMethods.isAvailable(method)) {
            val errorMessage = "The method $method does not exist/is not available"
            return Mono.just(
                InvalidCallContext(
                    CallError(requestItem.id, errorMessage, JsonRpcError(RpcResponseError.CODE_METHOD_NOT_EXIST, errorMessage))
                )
            )
        }
        // for ethereum the actual block needed for the call may be specified in the call parameters
        val callSpecificMatcher: Mono<Selector.Matcher> =
            if (BlockchainType.from(upstream.chain) == BlockchainType.ETHEREUM) {
                ethereumCallSelectors[chain]?.getMatcher(method, params, upstream.getHead())
            } else {
                null
            } ?: Mono.empty()
        return callSpecificMatcher.defaultIfEmpty(Selector.empty).map { csm ->
            val matcher = Selector.Builder()
                .withMatcher(csm)
                .forMethod(method)
                .forLabels(Selector.convertToMatcher(request.selector))

            val callQuorum = availableMethods.getQuorumFor(method) // can be null in tests
            callQuorum.init(upstream.getHead())

            // for NotLaggingQuorum it makes sense to select compatible upstreams before the call
            if (callQuorum is NotLaggingQuorum) {
                val lag = callQuorum.maxLag
                val minHeight = ((upstream.getHead().getCurrentHeight() ?: 0) - lag).coerceAtLeast(0)
                val heightMatcher = Selector.HeightMatcher(minHeight)
                matcher.withMatcher(heightMatcher)
            }
            val nonce = requestItem.nonce.let { if (it == 0L) null else it }
            ValidCallContext(requestItem.id, nonce, upstream, matcher.build(), callQuorum, RawCallDetails(method, params))
        }
    }

    fun fetch(ctx: ValidCallContext<ParsedCallDetails>): Mono<CallResult> {
        return ctx.upstream.getRoutedApi(ctx.matcher)
            .flatMap { api ->
                api.read(JsonRpcRequest(ctx.payload.method, ctx.payload.params, ctx.nonce))
                    .flatMap(JsonRpcResponse::requireResult)
                    .map {
                        CallResult.ok(ctx.id, ctx.nonce, it, null)
                    }
            }.switchIfEmpty(
                Mono.just(ctx).flatMap(this::executeOnRemote)
            )
            .onErrorResume {
                Mono.just(CallResult.fail(ctx.id, ctx.nonce, it))
            }
    }

    fun executeOnRemote(ctx: ValidCallContext<ParsedCallDetails>): Mono<CallResult> {
        // check if method is allowed to be executed at all
        if (!ctx.upstream.getMethods().isCallable(ctx.payload.method)) {
            return Mono.error(RpcException(RpcResponseError.CODE_METHOD_NOT_EXIST, "Unsupported method"))
        }
        val reader = quorumReaderFactory.create(ctx.getApis(), ctx.callQuorum, signer)
        return reader
            .read(JsonRpcRequest(ctx.payload.method, ctx.payload.params, ctx.nonce))
            .map {
                CallResult.ok(ctx.id, ctx.nonce, it.value, it.signature)
            }
            .onErrorResume { t ->
                Mono.just(CallResult.fail(ctx.id, ctx.nonce, t))
            }
            .switchIfEmpty(
                Mono.just(
                    CallResult.fail(
                        ctx.id, ctx.nonce,
                        CallError(1, "No response or no available upstream for ${ctx.payload.method}", null)
                    )
                )
            )
    }

    @Suppress("UNCHECKED_CAST")
    private fun extractParams(jsonParams: String): List<Any> {
        if (StringUtils.isEmpty(jsonParams)) {
            return emptyList()
        }
        val req = objectMapper.readValue(jsonParams, List::class.java)
        return req as List<Any>
    }

    interface CallContext {
        fun isValid(): Boolean
        fun <T> get(): ValidCallContext<T>
        fun getError(): CallError
    }

    open class ValidCallContext<T>(
        val id: Int,
        val nonce: Long?,
        val upstream: Multistream,
        val matcher: Selector.Matcher,
        val callQuorum: CallQuorum,
        val payload: T
    ) : CallContext {
        override fun isValid(): Boolean {
            return true
        }

        override fun <X> get(): ValidCallContext<X> {
            return this as ValidCallContext<X>
        }

        override fun getError(): CallError {
            throw IllegalStateException("Invalid context $id")
        }

        fun <X> withPayload(payload: X): ValidCallContext<X> {
            return ValidCallContext(id, nonce, upstream, matcher, callQuorum, payload)
        }

        fun getApis(): ApiSource {
            return upstream.getApiSource(matcher)
        }
    }

    /**
     * Call context when it's known in advance that the call is invalid and should return an error
     */
    open class InvalidCallContext(
        private val error: CallError
    ) : CallContext {
        override fun isValid(): Boolean {
            return false
        }

        override fun <T> get(): ValidCallContext<T> {
            throw IllegalStateException("Invalid context ${error.id}")
        }

        override fun getError(): CallError {
            return error
        }
    }

    open class CallFailure(val id: Int, val reason: Throwable) : Exception("Failed to call $id: ${reason.message}")

    open class CallError(val id: Int, val message: String, val upstreamError: JsonRpcError?) {
        companion object {
            fun from(t: Throwable): CallError {
                return when (t) {
                    is JsonRpcException -> CallError(t.id.asNumber().toInt(), t.error.message, t.error)
                    is RpcException -> CallError(t.code, t.rpcMessage, null)
                    is CallFailure -> CallError(t.id, t.reason.message ?: "Upstream Error", null)
                    else -> {
                        // May only happen if it's an unhandled exception.
                        // In this case try to find a meaningless details in the stack. Most important reason for doing that is to find an ID of the request
                        if (t.cause != null) {
                            from(t.cause!!)
                        } else {
                            CallError(1, t.message ?: "Upstream Error", null)
                        }
                    }
                }
            }
        }
    }

    open class CallResult(val id: Int, val nonce: Long?, val result: ByteArray?, val error: CallError?, val signature: ResponseSigner.Signature?) {
        companion object {
            fun ok(id: Int, nonce: Long?, result: ByteArray, signature: ResponseSigner.Signature?): CallResult {
                return CallResult(id, nonce, result, null, signature)
            }

            fun fail(id: Int, nonce: Long?, error: CallError): CallResult {
                return CallResult(id, nonce, null, error, null)
            }

            fun fail(id: Int, nonce: Long?, error: Throwable): CallResult {
                return CallResult(id, nonce, null, CallError.from(error), null)
            }
        }

        fun isError(): Boolean {
            return error != null
        }
    }

    class RawCallDetails(val method: String, val params: String)
    class ParsedCallDetails(val method: String, val params: List<Any>)
}
