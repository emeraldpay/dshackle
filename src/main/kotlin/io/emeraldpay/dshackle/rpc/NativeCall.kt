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
import io.emeraldpay.dshackle.BlockchainType
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.Global.Companion.nullValue
import io.emeraldpay.dshackle.SilentException
import io.emeraldpay.dshackle.commons.LOCAL_READER
import io.emeraldpay.dshackle.commons.REMOTE_QUORUM_RPC_READER
import io.emeraldpay.dshackle.commons.SPAN_ERROR
import io.emeraldpay.dshackle.commons.SPAN_REQUEST_ID
import io.emeraldpay.dshackle.commons.SPAN_RESPONSE_UPSTREAM_ID
import io.emeraldpay.dshackle.commons.SPAN_STATUS_MESSAGE
import io.emeraldpay.dshackle.config.MainConfig
import io.emeraldpay.dshackle.quorum.CallQuorum
import io.emeraldpay.dshackle.quorum.NotLaggingQuorum
import io.emeraldpay.dshackle.quorum.QuorumReaderFactory
import io.emeraldpay.dshackle.quorum.QuorumRpcReader
import io.emeraldpay.dshackle.reader.SpannedReader
import io.emeraldpay.dshackle.startup.UpstreamChangeEvent
import io.emeraldpay.dshackle.upstream.ApiSource
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.MultistreamHolder
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.calls.DefaultEthereumMethods
import io.emeraldpay.dshackle.upstream.calls.EthereumCallSelector
import io.emeraldpay.dshackle.upstream.ethereum.EthereumLikeMultistream
import io.emeraldpay.dshackle.upstream.ethereum.EthereumMultistream
import io.emeraldpay.dshackle.upstream.ethereum.EthereumPosMultiStream
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcError
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcException
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.dshackle.upstream.signature.ResponseSigner
import io.emeraldpay.etherjar.rpc.RpcException
import io.emeraldpay.etherjar.rpc.RpcResponseError
import io.micrometer.core.instrument.Metrics
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.springframework.cloud.sleuth.Span
import org.springframework.cloud.sleuth.Tracer
import org.springframework.cloud.sleuth.instrument.reactor.ReactorSleuth
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toMono
import reactor.util.context.Context
import java.util.EnumMap

@Service
open class NativeCall(
    private val multistreamHolder: MultistreamHolder,
    private val signer: ResponseSigner,
    config: MainConfig,
    private val tracer: Tracer
) {

    private val log = LoggerFactory.getLogger(NativeCall::class.java)
    private val objectMapper: ObjectMapper = Global.objectMapper

    private val localRouterEnabled = config.cache?.requestsCacheEnabled ?: true
    private val passthrough = config.passthrough

    var quorumReaderFactory: QuorumReaderFactory = QuorumReaderFactory.default()
    private val ethereumCallSelectors = EnumMap<Chain, EthereumCallSelector>(Chain::class.java)

    companion object {
        val casting: Map<BlockchainType, Class<out EthereumLikeMultistream>> = mapOf(
            BlockchainType.EVM_POS to EthereumPosMultiStream::class.java,
            BlockchainType.EVM_POW to EthereumMultistream::class.java
        )
    }

    @EventListener
    fun onUpstreamChangeEvent(event: UpstreamChangeEvent) {
        casting[BlockchainType.from(event.chain)]?.let { cast ->
            multistreamHolder.getUpstream(event.chain).let { up ->
                ethereumCallSelectors.putIfAbsent(
                    event.chain,
                    EthereumCallSelector(up.caches)
                )
            }
        }
    }

    open fun nativeCall(requestMono: Mono<BlockchainOuterClass.NativeCallRequest>): Flux<BlockchainOuterClass.NativeCallReplyItem> {
        return nativeCallResult(requestMono)
            .map(this::buildResponse)
            .onErrorResume(this::processException)
    }

    open fun nativeCallResult(requestMono: Mono<BlockchainOuterClass.NativeCallRequest>): Flux<CallResult> {
        val requestSpan = tracer.currentSpan()
        return requestMono.flatMapMany(this::prepareCall)
            .flatMap {
                val requestId = it.requestId
                val requestCount = it.requestCount
                val id = it.getContextId()
                val result = processCallContext(it, requestSpan)

                return@flatMap result
                    .onErrorResume { err ->
                        Mono.just(
                            CallResult.fail(id, 0, err, null)
                        )
                    }
                    .doOnNext { callRes -> completeSpan(callRes, requestCount) }
                    .contextWrite { ctx -> createTracingReactorContext(ctx, requestCount, requestId, requestSpan) }
            }
    }

    private fun completeSpan(callResult: CallResult, requestCount: Int) {
        val span = tracer.currentSpan()
        callResult.upstreamId?.let {
            span?.tag(SPAN_RESPONSE_UPSTREAM_ID, it)
        }
        if (callResult.isError()) {
            errorSpan(span, callResult.error?.message ?: "Internal error")
        }
        if (requestCount > 1) {
            span?.end()
        }
    }

    private fun errorSpan(span: Span?, message: String) {
        span?.apply {
            tag(SPAN_ERROR, "true")
            tag(SPAN_STATUS_MESSAGE, message)
        }
    }

    private fun createTracingReactorContext(
        ctx: Context,
        requestCount: Int,
        requestId: String,
        requestSpan: Span?
    ): Context {
        if (requestCount > 1) {
            val span = tracer.nextSpan(requestSpan)
                .name("emerald.blockchain/nativecall")
                .tag(SPAN_REQUEST_ID, requestId)
                .start()
            return ReactorSleuth.putSpanInScope(tracer, ctx, span)
        }
        return ctx
    }

    private fun processCallContext(
        callContext: CallContext,
        requestSpan: Span?
    ): Mono<CallResult> {
        return if (callContext.isValid()) {
            run {
                val parsed = try {
                    parseParams(callContext.get())
                } catch (e: Exception) {
                    return@run Mono.error(e)
                }
                if (callContext.requestCount == 1 && callContext.requestId.isNotBlank()) {
                    requestSpan?.tag(SPAN_REQUEST_ID, callContext.requestId)
                }
                this.fetch(parsed)
                    .doOnError { e -> log.warn("Error during native call: ${e.message}") }
            }
        } else {
            val error = callContext.getError()

            Mono.just(
                CallResult(error.id, 0, null, error, null, null, null)
            )
        }
    }

    fun parseParams(it: ValidCallContext<RawCallDetails>): ValidCallContext<ParsedCallDetails> {
        val rawParams = extractParams(it.payload.params)
        val params = it.requestDecorator.processRequest(rawParams)
        return it.withPayload(ParsedCallDetails(it.payload.method, params))
    }

    fun buildResponse(it: CallResult): BlockchainOuterClass.NativeCallReplyItem {
        val result = BlockchainOuterClass.NativeCallReplyItem.newBuilder()
            .setSucceed(!it.isError())
            .setId(it.id)
        if (it.isError()) {
            it.error?.let { error ->
                result.setErrorMessage(error.message)
                    .setErrorCode(error.id)

                error.data?.let { data ->
                    result.setErrorData(data)
                }
            }
        } else {
            result.payload = ByteString.copyFrom(it.result)
        }
        if (it.nonce != null && it.signature != null) {
            result.signature = buildSignature(it.nonce, it.signature)
        }
        it.upstreamId ?.let { result.upstreamId = it }
        return result.build()
    }

    fun buildSignature(
        nonce: Long,
        signature: ResponseSigner.Signature
    ): BlockchainOuterClass.NativeCallReplySignature {
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
        val message = it?.message ?: "Internal error"
        errorSpan(tracer.currentSpan(), message)
        return BlockchainOuterClass.NativeCallReplyItem.newBuilder()
            .setSucceed(false)
            .setErrorMessage(message)
            .setErrorCode(500)
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

        val matcher = Selector.convertToMatcher(request.selector)

        if (!multistreamHolder.getUpstream(chain).hasMatchingUpstream(matcher)) {
            if (Global.metricsExtended) {
                Metrics.globalRegistry
                    .counter("no_matching_upstream", "chain", chain.chainCode, "matcher", matcher.describeInternal())
                    .increment()
            }
            return Flux.error(CallFailure(0, SilentException.NoMatchingUpstream(matcher)))
        }

        val upstream = multistreamHolder.getUpstream(chain)

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
        val requestId = requestItem.requestId
        val requestCount = request.itemsCount
        val method = requestItem.method
        val params = requestItem.payload.toStringUtf8()
        val availableMethods = upstream.getMethods()

        if (!availableMethods.isAvailable(method)) {
            val errorMessage = "The method $method is not available"
            return Mono.just(
                InvalidCallContext(
                    CallError(
                        requestItem.id,
                        errorMessage,
                        JsonRpcError(RpcResponseError.CODE_METHOD_NOT_EXIST, errorMessage),
                        null
                    ),
                    requestId,
                    requestCount
                )
            )
        }
        // for ethereum the actual block needed for the call may be specified in the call parameters
        val callSpecificMatcher: Mono<Selector.Matcher> =
            if (BlockchainType.from(upstream.chain) == BlockchainType.EVM_POS || BlockchainType.from(upstream.chain) == BlockchainType.EVM_POW) {
                ethereumCallSelectors[chain]?.getMatcher(method, params, upstream.getHead(), passthrough)
            } else {
                null
            } ?: Mono.empty()
        return callSpecificMatcher.defaultIfEmpty(Selector.empty).map { csm ->
            val matcher = Selector.Builder()
                .withMatcher(csm)
                .forMethod(method)
                .forLabels(Selector.convertToMatcher(request.selector))

            val callQuorum = availableMethods.createQuorumFor(method) // can be null in tests
            callQuorum.init(upstream.getHead())

            // for NotLaggingQuorum it makes sense to select compatible upstreams before the call
            if (callQuorum is NotLaggingQuorum) {
                val lag = callQuorum.maxLag
                val minHeight = ((upstream.getHead().getCurrentHeight() ?: 0) - lag).coerceAtLeast(0)
                val heightMatcher = Selector.HeightMatcher(minHeight)
                matcher.withMatcher(heightMatcher)
            }
            val nonce = requestItem.nonce.let { if (it == 0L) null else it }
            val requestDecorator = getRequestDecorator(requestItem.method)
            val resultDecorator = getResultDecorator(requestItem.method)

            val selector = request.takeIf { it.hasSelector() }?.let { Selectors.keepForwarded(it.selector) }

            ValidCallContext(
                requestItem.id,
                nonce,
                upstream,
                matcher.build(),
                callQuorum,
                RawCallDetails(method, params),
                requestDecorator,
                resultDecorator,
                selector,
                requestId,
                requestCount
            )
        }
    }

    private fun getRequestDecorator(method: String): RequestDecorator =
        if (method in DefaultEthereumMethods.withFilterIdMethods)
            WithFilterIdDecorator()
        else
            NoneRequestDecorator()

    private fun getResultDecorator(method: String): ResultDecorator =
        if (method in DefaultEthereumMethods.newFilterMethods) CreateFilterDecorator() else NoneResultDecorator()

    fun fetch(ctx: ValidCallContext<ParsedCallDetails>): Mono<CallResult> {
        return ctx.upstream.getLocalReader(localRouterEnabled)
            .flatMap { api ->
                SpannedReader(api, tracer, LOCAL_READER)
                    .read(JsonRpcRequest(ctx.payload.method, ctx.payload.params, ctx.nonce, ctx.forwardedSelector))
                    .flatMap(JsonRpcResponse::requireResult)
                    .map {
                        validateResult(it, "local", ctx)
                        if (ctx.nonce != null) {
                            CallResult.ok(ctx.id, ctx.nonce, it, signer.sign(ctx.nonce, it, ctx.upstream.getId()), ctx.upstream.getId(), ctx)
                        } else {
                            CallResult.ok(ctx.id, null, it, null, ctx.upstream.getId(), ctx)
                        }
                    }
            }.switchIfEmpty(
                Mono.just(ctx).flatMap(this::executeOnRemote)
            )
            .onErrorResume {
                Mono.just(CallResult.fail(ctx.id, ctx.nonce, it, ctx))
            }
    }

    fun executeOnRemote(ctx: ValidCallContext<ParsedCallDetails>): Mono<CallResult> {
        // check if method is allowed to be executed at all
        if (!ctx.upstream.getMethods().isCallable(ctx.payload.method)) {
            return Mono.error(RpcException(RpcResponseError.CODE_METHOD_NOT_EXIST, "Unsupported method"))
        }
        val reader = quorumReaderFactory.create(ctx.getApis(), ctx.callQuorum, signer, tracer)
        val counter = reader.attempts()

        return SpannedReader(reader, tracer, REMOTE_QUORUM_RPC_READER)
            .read(JsonRpcRequest(ctx.payload.method, ctx.payload.params, ctx.nonce, ctx.forwardedSelector))
            .map {
                val bytes = ctx.resultDecorator.processResult(it)
                validateResult(bytes, "remote", ctx)
                val upId = it.providedUpstreamId
                    ?: if (it.resolvers.isEmpty()) ctx.upstream.getId() else it.resolvers.first().getId()
                CallResult.ok(ctx.id, ctx.nonce, bytes, it.signature, upId, ctx)
            }
            .onErrorResume { t ->
                Mono.just(CallResult.fail(ctx.id, ctx.nonce, t, ctx))
            }
            .switchIfEmpty(
                Mono.fromSupplier {
                    counter.get().let { attempts ->
                        CallResult.fail(
                            ctx.id, ctx.nonce,
                            CallError(1, "No response or no available upstream for ${ctx.payload.method}", null, null),
                            ctx
                        ).also {
                            countFailure(attempts, ctx)
                        }
                    }
                }
            )
    }

    private fun validateResult(bytes: ByteArray, origin: String, ctx: ValidCallContext<ParsedCallDetails>) {
        if (bytes.isEmpty() || nullValue.contentEquals(bytes))
            log.warn("Empty result from origin $origin, method ${ctx.payload.method}, params ${ctx.payload.params}")
    }

    private fun errorMessage(attempts: Int, method: String): String =
        when (attempts) {
            -1 -> "No response or no available upstream for $method"
            0 -> "No available upstream for $method"
            else -> "No response for $method"
        }

    private fun countFailure(counter: Int, ctx: ValidCallContext<ParsedCallDetails>) =
        when (counter) {
            -1 -> "UNDEFINED"
            0 -> "NO_AVAIL_UPSTREAM"
            else -> "NO_RESPONSE"
        }.let { reason ->
            if (Global.metricsExtended) {
                Metrics.globalRegistry.counter(
                    "native_call_failure",
                    "upstream",
                    ctx.upstream.getId(),
                    "reason",
                    reason,
                    "chain",
                    ctx.upstream.chain.chainCode,
                ).increment()
            }
        }

    @Suppress("UNCHECKED_CAST")
    private fun extractParams(jsonParams: String): List<Any> {
        if (StringUtils.isEmpty(jsonParams) || jsonParams == "null") {
            return emptyList()
        }
        val req = objectMapper.readValue(jsonParams, List::class.java)
        return req as List<Any>
    }

    abstract class CallContext(
        val requestId: String,
        val requestCount: Int
    ) {
        abstract fun isValid(): Boolean
        abstract fun <T> get(): ValidCallContext<T>
        abstract fun getError(): CallError
        abstract fun getContextId(): Int
    }

    interface ResultDecorator {
        fun processResult(result: QuorumRpcReader.Result): ByteArray
    }

    open class NoneResultDecorator : ResultDecorator {
        override fun processResult(result: QuorumRpcReader.Result): ByteArray = result.value
    }

    open class CreateFilterDecorator : ResultDecorator {

        companion object {
            const val quoteCode = '"'.code.toByte()
        }
        override fun processResult(result: QuorumRpcReader.Result): ByteArray {
            val bytes = result.value
            if (bytes.last() == quoteCode) {
                val suffix = result.resolvers
                    .map { it.nodeId() }
                    .first()
                    .toUByte()
                    .toString(16).padStart(2, padChar = '0').toByteArray()
                bytes[bytes.lastIndex] = suffix.first()
                return bytes + suffix.last() + quoteCode
            }
            return bytes
        }
    }

    interface RequestDecorator {
        fun processRequest(request: List<Any>): List<Any>
    }

    open class NoneRequestDecorator : RequestDecorator {
        override fun processRequest(request: List<Any>): List<Any> = request
    }

    open class WithFilterIdDecorator : RequestDecorator {
        override fun processRequest(request: List<Any>): List<Any> {
            val filterId = request.first().toString()
            val sanitized = filterId.substring(0, filterId.lastIndex - 1)
            return listOf(sanitized)
        }
    }

    open class ValidCallContext<T>(
        val id: Int,
        val nonce: Long?,
        val upstream: Multistream,
        val matcher: Selector.Matcher,
        val callQuorum: CallQuorum,
        val payload: T,
        val requestDecorator: RequestDecorator,
        val resultDecorator: ResultDecorator,
        val forwardedSelector: BlockchainOuterClass.Selector?,
        requestId: String,
        requestCount: Int
    ) : CallContext(requestId, requestCount) {

        constructor(
            id: Int,
            nonce: Long?,
            upstream: Multistream,
            matcher: Selector.Matcher,
            callQuorum: CallQuorum,
            payload: T,
            requestId: String,
            requestCount: Int
        ) : this(
            id, nonce, upstream, matcher, callQuorum, payload,
            NoneRequestDecorator(), NoneResultDecorator(), null, requestId, requestCount
        )

        override fun isValid(): Boolean {
            return true
        }

        override fun <X> get(): ValidCallContext<X> {
            return this as ValidCallContext<X>
        }

        override fun getError(): CallError {
            throw IllegalStateException("Invalid context $id")
        }

        override fun getContextId(): Int = id

        fun <X> withPayload(payload: X): ValidCallContext<X> {
            return ValidCallContext(
                id, nonce, upstream, matcher, callQuorum, payload,
                requestDecorator, resultDecorator, forwardedSelector, requestId, requestCount
            )
        }

        fun getApis(): ApiSource {
            return upstream.getApiSource(matcher)
        }
    }

    /**
     * Call context when it's known in advance that the call is invalid and should return an error
     */
    open class InvalidCallContext(
        private val error: CallError,
        requestId: String,
        requestCount: Int
    ) : CallContext(requestId, requestCount) {
        override fun isValid(): Boolean {
            return false
        }

        override fun <T> get(): ValidCallContext<T> {
            throw IllegalStateException("Invalid context ${error.id}")
        }

        override fun getError(): CallError {
            return error
        }

        override fun getContextId(): Int = error.id
    }

    open class CallFailure(val id: Int, val reason: Throwable) : Exception("Failed to call $id: ${reason.message}")

    open class CallError(val id: Int, val message: String, val upstreamError: JsonRpcError?, val data: String?) {

        companion object {

            private val log = LoggerFactory.getLogger(CallError::class.java)
            private fun getDataAsSting(details: Any?): String? {
                return when (details) {
                    is String -> details
                    null -> null
                    else -> {
                        log.debug("Unsupported error details: {}", details)
                        null
                    }
                }
            }
            fun from(t: Throwable): CallError {
                return when (t) {
                    is JsonRpcException -> CallError(t.error.code, t.error.message, t.error, getDataAsSting(t.error.details))
                    is RpcException -> CallError(t.code, t.rpcMessage, null, getDataAsSting(t.details))
                    is CallFailure -> CallError(t.id, t.reason.message ?: "Upstream Error", null, null)
                    else -> {
                        // May only happen if it's an unhandled exception.
                        // In this case try to find a meaningless details in the stack. Most important reason for doing that is to find an ID of the request
                        if (t.cause != null) {
                            from(t.cause!!)
                        } else {
                            CallError(1, t.message ?: "Upstream Error", null, null)
                        }
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
        val upstreamId: String?,
        val ctx: ValidCallContext<ParsedCallDetails>?
    ) {
        companion object {
            fun ok(id: Int, nonce: Long?, result: ByteArray, signature: ResponseSigner.Signature?, upstreamId: String?, ctx: ValidCallContext<ParsedCallDetails>?): CallResult {
                return CallResult(id, nonce, result, null, signature, upstreamId, ctx)
            }

            fun fail(id: Int, nonce: Long?, error: CallError, ctx: ValidCallContext<ParsedCallDetails>?): CallResult {
                return CallResult(id, nonce, null, error, null, null, ctx)
            }

            fun fail(id: Int, nonce: Long?, error: Throwable, ctx: ValidCallContext<ParsedCallDetails>?): CallResult {
                return CallResult(id, nonce, null, CallError.from(error), null, null, ctx)
            }
        }

        fun isError(): Boolean {
            return error != null
        }
    }

    class RawCallDetails(val method: String, val params: String)
    class ParsedCallDetails(val method: String, val params: List<Any>)
}
