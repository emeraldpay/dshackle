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
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.Global.Companion.nullValue
import io.emeraldpay.dshackle.SilentException
import io.emeraldpay.dshackle.commons.LOCAL_READER
import io.emeraldpay.dshackle.commons.RPC_READER
import io.emeraldpay.dshackle.commons.SPAN_ERROR
import io.emeraldpay.dshackle.commons.SPAN_REQUEST_CANCELLED
import io.emeraldpay.dshackle.commons.SPAN_REQUEST_ID
import io.emeraldpay.dshackle.commons.SPAN_STATUS_MESSAGE
import io.emeraldpay.dshackle.config.MainConfig
import io.emeraldpay.dshackle.quorum.CallQuorum
import io.emeraldpay.dshackle.quorum.NotLaggingQuorum
import io.emeraldpay.dshackle.reader.RequestReader
import io.emeraldpay.dshackle.reader.RequestReaderFactory
import io.emeraldpay.dshackle.reader.RequestReaderFactory.ReaderData
import io.emeraldpay.dshackle.reader.SpannedReader
import io.emeraldpay.dshackle.upstream.ApiSource
import io.emeraldpay.dshackle.upstream.ChainCallError
import io.emeraldpay.dshackle.upstream.ChainException
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.MultistreamHolder
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.calls.DefaultEthereumMethods
import io.emeraldpay.dshackle.upstream.calls.DisabledCallMethods
import io.emeraldpay.dshackle.upstream.ethereum.rpc.RpcException
import io.emeraldpay.dshackle.upstream.ethereum.rpc.RpcResponseError
import io.emeraldpay.dshackle.upstream.finalization.FinalizationData
import io.emeraldpay.dshackle.upstream.rpcclient.CallParams
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import io.emeraldpay.dshackle.upstream.rpcclient.ObjectParams
import io.emeraldpay.dshackle.upstream.rpcclient.RestParams
import io.emeraldpay.dshackle.upstream.signature.ResponseSigner
import io.emeraldpay.dshackle.upstream.stream.Chunk
import io.micrometer.core.instrument.Metrics
import org.apache.commons.lang3.StringUtils
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
import org.springframework.cloud.sleuth.Span
import org.springframework.cloud.sleuth.Tracer
import org.springframework.cloud.sleuth.instrument.reactor.ReactorSleuth
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toMono
import reactor.util.context.Context

@Service
open class NativeCall(
    private val multistreamHolder: MultistreamHolder,
    private val signer: ResponseSigner,
    config: MainConfig,
    private val tracer: Tracer,
) {

    private val log = LoggerFactory.getLogger(NativeCall::class.java)
    private val objectMapper: ObjectMapper = Global.objectMapper

    private val passthrough = config.passthrough

    var requestReaderFactory: RequestReaderFactory = RequestReaderFactory.default()

    open fun nativeCall(requestMono: Mono<BlockchainOuterClass.NativeCallRequest>): Flux<BlockchainOuterClass.NativeCallReplyItem> {
        return nativeCallResult(requestMono)
            .flatMapSequential(this::processCallResult)
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
                            CallResult.fail(id, 0, err, null),
                        )
                    }
                    .doOnNext {
                            callRes ->
                        if (callRes.error?.message?.contains(Regex("method ([A-Za-z0-9_]+) does not exist/is not available")) == true) {
                            if (it is ValidCallContext<*>) {
                                if (it.payload is ParsedCallDetails) {
                                    log.error("nativeCallResult method ${it.payload.method} of ${it.upstream.getId()} is not available, disabling")
                                    val cm = (it.upstream.getMethods() as DisabledCallMethods)
                                    cm.disableMethodTemporarily(it.payload.method)
                                    it.upstream.updateMethods(cm)
                                }
                            }
                        }
                        completeSpan(callRes, requestCount)
                    }
                    .doOnCancel {
                        tracer.currentSpan()?.tag(SPAN_STATUS_MESSAGE, SPAN_REQUEST_CANCELLED)?.end()
                    }
                    .contextWrite { ctx -> createTracingReactorContext(ctx, requestCount, requestId, requestSpan) }
            }
    }

    private fun processCallResult(callResult: CallResult): Publisher<BlockchainOuterClass.NativeCallReplyItem> {
        return if (callResult.stream == null) {
            Mono.just(buildResponse(callResult))
        } else {
            return callResult.stream.switchOnFirst { t, stream ->
                val firstChunk = t.get()
                if (firstChunk == null) {
                    stream.map { buildStreamResult(it, callResult.id).build() }
                } else {
                    Flux.concat(
                        Mono.just(firstChunk)
                            .map {
                                val result = buildStreamResult(it, callResult.id)
                                if (callResult.upstreamSettingsData.isNotEmpty()) {
                                    getUpstreamIdsAndVersions(callResult.upstreamSettingsData)
                                        .let { idsAndVersions ->
                                            result.upstreamId = idsAndVersions.first
                                            result.upstreamNodeVersion = idsAndVersions.second
                                        }
                                }
                                result.build()
                            },
                        stream.skip(1).map { buildStreamResult(it, callResult.id).build() },
                    )
                }
            }
        }
    }

    private fun buildStreamResult(chunk: Chunk, id: Int): BlockchainOuterClass.NativeCallReplyItem.Builder {
        return BlockchainOuterClass.NativeCallReplyItem.newBuilder()
            .setSucceed(true)
            .setFinalChunk(chunk.finalChunk)
            .setChunked(true)
            .setPayload(ByteString.copyFrom(chunk.chunkData))
            .setId(id)
    }

    private fun completeSpan(callResult: CallResult, requestCount: Int) {
        val span = tracer.currentSpan()
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
        requestSpan: Span?,
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
        requestSpan: Span?,
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
                CallResult(error.id, 0, null, error, null, emptyList(), null),
            )
        }
    }

    fun parseParams(it: ValidCallContext<ParsedCallDetails>): ValidCallContext<ParsedCallDetails> {
        val params = it.requestDecorator.processRequest(it.payload.params)
        return it.withPayload(ParsedCallDetails(it.payload.method, params))
    }

    fun buildResponse(it: CallResult): BlockchainOuterClass.NativeCallReplyItem {
        val result = BlockchainOuterClass.NativeCallReplyItem.newBuilder()
            .setSucceed(!it.isError())
            .setId(it.id)
        if (it.isError()) {
            it.error?.let { error ->
                result.setErrorMessage(error.message)
                    .setItemErrorCode(error.id)

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
        if (it.upstreamSettingsData.isNotEmpty()) {
            val idsAndVersions = getUpstreamIdsAndVersions(it.upstreamSettingsData)
            result.upstreamId = idsAndVersions.first
            result.upstreamNodeVersion = idsAndVersions.second
        }
        it.finalization?.let {
            result.finalization = Common.FinalizationData.newBuilder()
                .setHeight(it.height)
                .setType(it.type.toProtoFinalizationType())
                .build()
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
        upstream: Multistream,
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
        upstream: Multistream,
    ): Mono<CallContext> {
        val requestId = requestItem.requestId
        val requestCount = request.itemsCount
        val method = requestItem.method
        val params = if (requestItem.hasPayload()) {
            requestItem.payload.toStringUtf8()
        } else {
            ""
        }
        val availableMethods = upstream.getMethods()
        if (!availableMethods.isAvailable(method)) {
            val errorMessage = "The method $method is not available"
            return Mono.just(
                InvalidCallContext(
                    CallError(
                        requestItem.id,
                        errorMessage,
                        ChainCallError(RpcResponseError.CODE_METHOD_NOT_EXIST, errorMessage),
                        null,
                    ),
                    requestId,
                    requestCount,
                ),
            )
        }
        val upstreamFilter = requestItem.selectorsList
            .takeIf { it.isNotEmpty() }
            ?.run { Selector.convertToUpstreamFilter(this) }
        // for ethereum the actual block needed for the call may be specified in the call parameters
        val callSpecificMatcher: Mono<Selector.Matcher> =
            upstreamFilter?.matcher?.let { Mono.just(it) } ?: upstream.callSelector?.getMatcher(method, params, upstream.getHead(), passthrough) ?: Mono.empty()
        return callSpecificMatcher.defaultIfEmpty(Selector.empty).map { csm ->
            val matcher = Selector.Builder()
                .withMatcher(csm)
                .forMethod(method)
                .forLabels(Selector.convertToMatcher(request.selector))

            val callQuorum = availableMethods.createQuorumFor(method) // can be null in tests
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

            val isStreamRequest = request.chunkSize != 0

            ValidCallContext(
                requestItem.id,
                nonce,
                upstream,
                Selector.UpstreamFilter(upstreamFilter?.sort ?: Selector.Sort.default, matcher.build()),
                callQuorum,
                parsedCallDetails(requestItem),
                requestDecorator,
                resultDecorator,
                selector,
                isStreamRequest,
                requestId,
                requestCount,
            )
        }
    }

    private fun getUpstreamIdsAndVersions(upstreamSettingsData: List<Upstream.UpstreamSettingsData>): Pair<String, String> {
        return Pair(
            upstreamSettingsData.joinToString { it.id },
            upstreamSettingsData.joinToString { it.nodeVersion },
        )
    }

    private fun parsedCallDetails(item: BlockchainOuterClass.NativeCallItem): ParsedCallDetails {
        return if (item.hasPayload()) {
            ParsedCallDetails(item.method, extractParams(item.payload.toStringUtf8()))
        } else if (item.hasRestData()) {
            ParsedCallDetails(
                item.method,
                RestParams(
                    item.restData.headersList.map { Pair(it.key, it.value) },
                    item.restData.queryParamsList.map { Pair(it.key, it.value) },
                    item.restData.pathParamsList,
                    item.restData.payload.toByteArray(),
                ),
            )
        } else {
            throw IllegalStateException("Wrong payload type")
        }
    }

    private fun getRequestDecorator(method: String): RequestDecorator =
        if (method in DefaultEthereumMethods.withFilterIdMethods) {
            WithFilterIdDecorator()
        } else {
            NoneRequestDecorator()
        }

    private fun getResultDecorator(method: String): ResultDecorator =
        if (method in DefaultEthereumMethods.newFilterMethods) CreateFilterDecorator() else NoneResultDecorator()

    fun fetch(ctx: ValidCallContext<ParsedCallDetails>): Mono<CallResult> {
        return ctx.upstream.getLocalReader()
            .flatMap { api ->
                SpannedReader(api, tracer, LOCAL_READER)
                    .read(ctx.payload.toChainRequest(ctx.nonce, ctx.forwardedSelector, false))
                    .map {
                        val result = it.getResult()
                        val resolvedUpstreamData = it.resolvedUpstreamData.ifEmpty {
                            ctx.upstream.getUpstreamSettingsData()?.run { listOf(this) } ?: emptyList()
                        }
                        validateResult(result, "local", ctx)
                        if (ctx.nonce != null) {
                            val source = if (resolvedUpstreamData.isNotEmpty()) {
                                resolvedUpstreamData[0].id
                            } else {
                                ctx.upstream.getId()
                            }
                            CallResult.ok(ctx.id, ctx.nonce, result, signer.sign(ctx.nonce, result, source), resolvedUpstreamData, ctx, it.finalization)
                        } else {
                            CallResult.ok(ctx.id, null, result, null, resolvedUpstreamData, ctx, it.finalization)
                        }
                    }
            }.switchIfEmpty(
                Mono.just(ctx).flatMap(this::executeOnRemote),
            )
            .onErrorResume {
                Mono.just(CallResult.fail(ctx.id, ctx.nonce, it, ctx))
            }.doOnNext {
                if (it.finalization != null && it.upstreamSettingsData.isNotEmpty()) {
                    ctx.upstream.addFinalization(it.finalization, it.upstreamSettingsData[0].id)
                }
            }
    }

    fun executeOnRemote(ctx: ValidCallContext<ParsedCallDetails>): Mono<CallResult> {
        // check if method is allowed to be executed at all
        if (!ctx.upstream.getMethods().isCallable(ctx.payload.method)) {
            return Mono.error(RpcException(RpcResponseError.CODE_METHOD_NOT_EXIST, "Unsupported method"))
        }
        val reader = requestReaderFactory.create(
            ReaderData(ctx.upstream, ctx.upstreamFilter, ctx.callQuorum, signer, tracer),
        )
        val counter = reader.attempts()

        return SpannedReader(reader, tracer, RPC_READER)
            .read(ctx.payload.toChainRequest(ctx.nonce, ctx.forwardedSelector, ctx.streamRequest))
            .map {
                val resolvedUpstreamData = it.resolvedUpstreamData.ifEmpty {
                    ctx.upstream.getUpstreamSettingsData()?.run { listOf(this) } ?: emptyList()
                }
                if (it.stream == null) {
                    val bytes = ctx.resultDecorator.processResult(it)
                    validateResult(bytes, "remote", ctx)
                    CallResult.ok(ctx.id, ctx.nonce, bytes, it.signature, resolvedUpstreamData, ctx)
                } else {
                    CallResult.ok(ctx.id, ctx.nonce, ByteArray(0), it.signature, resolvedUpstreamData, ctx, it.stream)
                }
            }
            .onErrorResume { t ->
                Mono.just(CallResult.fail(ctx.id, ctx.nonce, t, ctx))
            }
            .switchIfEmpty(
                Mono.fromSupplier {
                    counter.get().let { attempts ->
                        CallResult.fail(
                            ctx.id,
                            ctx.nonce,
                            CallError(1, "No response or no available upstream for ${ctx.payload.method}", null, null),
                            ctx,
                        ).also {
                            countFailure(attempts, ctx)
                        }
                    }
                },
            )
    }

    private fun validateResult(bytes: ByteArray, origin: String, ctx: ValidCallContext<ParsedCallDetails>) {
        if (bytes.isEmpty() || nullValue.contentEquals(bytes)) {
            log.warn("Empty result from origin $origin, method ${ctx.payload.method}, params ${ctx.payload.params}")
        }
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
                    ctx.upstream.getChain().chainCode,
                ).increment()
            }
        }

    @Suppress("UNCHECKED_CAST")
    private fun extractParams(jsonParams: String): CallParams {
        if (StringUtils.isEmpty(jsonParams) || jsonParams == "null") {
            return ListParams()
        }
        if (jsonParams.trimStart().startsWith("{")) {
            val req = objectMapper.readValue(jsonParams, Map::class.java)
            return ObjectParams(req as Map<Any, Any>)
        }
        val req = objectMapper.readValue(jsonParams, List::class.java)
        return ListParams(req as List<Any>)
    }

    abstract class CallContext(
        val requestId: String,
        val requestCount: Int,
    ) {
        abstract fun isValid(): Boolean
        abstract fun <T> get(): ValidCallContext<T>
        abstract fun getError(): CallError
        abstract fun getContextId(): Int
    }

    interface ResultDecorator {
        fun processResult(result: RequestReader.Result): ByteArray
    }

    open class NoneResultDecorator : ResultDecorator {
        override fun processResult(result: RequestReader.Result): ByteArray = result.value
    }

    open class CreateFilterDecorator : ResultDecorator {

        companion object {
            const val quoteCode = '"'.code.toByte()
        }
        override fun processResult(result: RequestReader.Result): ByteArray {
            val bytes = result.value
            if (bytes.last() == quoteCode && result.resolvedUpstreamData.isNotEmpty()) {
                val suffix = result.resolvedUpstreamData[0].nodeId
                    .toUShort()
                    .toString(16).padStart(4, padChar = '0').toByteArray()
                return resultArray(bytes, suffix)
            }
            return bytes
        }

        private fun resultArray(bytes: ByteArray, suffix: ByteArray): ByteArray {
            val newBytes = ByteArray(bytes.size + suffix.size)
            newBytes[newBytes.size - 1] = quoteCode
            var index = bytes.size - 1

            System.arraycopy(bytes, 0, newBytes, 0, bytes.size - 1)
            for (byteVal in suffix) newBytes[index++] = byteVal

            return newBytes
        }
    }

    interface RequestDecorator {
        fun processRequest(request: CallParams): CallParams
    }

    open class NoneRequestDecorator : RequestDecorator {
        override fun processRequest(request: CallParams): CallParams = request
    }

    open class WithFilterIdDecorator : RequestDecorator {
        override fun processRequest(request: CallParams): CallParams {
            if (request is ListParams) {
                val filterId = request.list.first().toString()
                val sanitized = filterId.substring(0, filterId.lastIndex - 3)
                return ListParams(listOf(sanitized))
            }
            return request
        }
    }

    open class ValidCallContext<T>(
        val id: Int,
        val nonce: Long?,
        val upstream: Multistream,
        val upstreamFilter: Selector.UpstreamFilter,
        val callQuorum: CallQuorum,
        val payload: T,
        val requestDecorator: RequestDecorator,
        val resultDecorator: ResultDecorator,
        val forwardedSelector: BlockchainOuterClass.Selector?,
        val streamRequest: Boolean,
        requestId: String,
        requestCount: Int,
    ) : CallContext(requestId, requestCount) {

        constructor(
            id: Int,
            nonce: Long?,
            upstream: Multistream,
            upstreamFilter: Selector.UpstreamFilter,
            callQuorum: CallQuorum,
            payload: T,
            requestId: String,
            requestCount: Int,
        ) : this(
            id, nonce, upstream, upstreamFilter, callQuorum, payload,
            NoneRequestDecorator(), NoneResultDecorator(), null, false, requestId, requestCount,
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
                id, nonce, upstream, upstreamFilter, callQuorum, payload,
                requestDecorator, resultDecorator, forwardedSelector, streamRequest, requestId, requestCount,
            )
        }

        fun getApis(): ApiSource {
            return upstream.getApiSource(upstreamFilter)
        }
    }

    /**
     * Call context when it's known in advance that the call is invalid and should return an error
     */
    open class InvalidCallContext(
        private val error: CallError,
        requestId: String,
        requestCount: Int,
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

    data class CallError(
        val id: Int,
        val message: String,
        val upstreamError: ChainCallError?,
        val data: String?,
        val upstreamSettingsData: List<Upstream.UpstreamSettingsData> = emptyList(),
    ) {

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
                    is ChainException -> CallError(t.error.code, t.error.message, t.error, getDataAsSting(t.error.details), t.upstreamSettingsData)
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

    open class CallResult @JvmOverloads constructor(
        val id: Int,
        val nonce: Long?,
        val result: ByteArray?,
        val error: CallError?,
        val signature: ResponseSigner.Signature?,
        val upstreamSettingsData: List<Upstream.UpstreamSettingsData>,
        val ctx: ValidCallContext<ParsedCallDetails>?,
        val stream: Flux<Chunk>? = null,
        val finalization: FinalizationData? = null,
    ) {

        constructor(
            id: Int,
            nonce: Long?,
            result: ByteArray?,
            callError: CallError?,
            signature: ResponseSigner.Signature?,
            ctx: ValidCallContext<ParsedCallDetails>?,
        ) : this(id, nonce, result, callError, signature, callError?.upstreamSettingsData ?: emptyList(), ctx)

        companion object {
            fun ok(id: Int, nonce: Long?, result: ByteArray, signature: ResponseSigner.Signature?, upstreamSettingsData: List<Upstream.UpstreamSettingsData>, ctx: ValidCallContext<ParsedCallDetails>?): CallResult {
                return CallResult(id, nonce, result, null, signature, upstreamSettingsData, ctx)
            }

            fun ok(id: Int, nonce: Long?, result: ByteArray, signature: ResponseSigner.Signature?, upstreamSettingsData: List<Upstream.UpstreamSettingsData>, ctx: ValidCallContext<ParsedCallDetails>?, final: FinalizationData?): CallResult {
                return CallResult(id, nonce, result, null, signature, upstreamSettingsData, ctx, null, final)
            }

            fun ok(id: Int, nonce: Long?, result: ByteArray, signature: ResponseSigner.Signature?, upstreamSettingsData: List<Upstream.UpstreamSettingsData>, ctx: ValidCallContext<ParsedCallDetails>?, stream: Flux<Chunk>?): CallResult {
                return CallResult(id, nonce, result, null, signature, upstreamSettingsData, ctx, stream)
            }

            fun fail(id: Int, nonce: Long?, error: CallError, ctx: ValidCallContext<ParsedCallDetails>?): CallResult {
                return CallResult(id, nonce, null, error, null, emptyList(), ctx)
            }

            fun fail(id: Int, nonce: Long?, error: Throwable, ctx: ValidCallContext<ParsedCallDetails>?): CallResult {
                return CallResult(id, nonce, null, CallError.from(error), null, ctx)
            }
        }

        fun isError(): Boolean {
            return error != null
        }
    }

    class ParsedCallDetails(val method: String, val params: CallParams) {
        fun toChainRequest(
            nonce: Long?,
            selector: BlockchainOuterClass.Selector?,
            streamRequest: Boolean,
        ): ChainRequest {
            return ChainRequest(method, params, nonce, selector, streamRequest)
        }
    }
}
