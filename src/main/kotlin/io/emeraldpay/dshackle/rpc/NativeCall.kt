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
import io.emeraldpay.dshackle.upstream.*
import io.emeraldpay.dshackle.quorum.AlwaysQuorum
import io.emeraldpay.dshackle.quorum.CallQuorum
import io.emeraldpay.dshackle.quorum.NotLaggingQuorum
import io.emeraldpay.dshackle.quorum.QuorumReaderFactory
import io.emeraldpay.dshackle.upstream.calls.EthereumCallSelector
import io.emeraldpay.dshackle.upstream.ethereum.EthereumMultistream
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcError
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcException
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.grpc.BlockchainType
import io.emeraldpay.grpc.Chain
import io.emeraldpay.etherjar.rpc.RpcException
import io.emeraldpay.etherjar.rpc.RpcResponseError
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.*
import reactor.kotlin.core.publisher.toMono
import java.lang.Exception
import java.util.*

@Service
open class NativeCall(
        @Autowired private val multistreamHolder: MultistreamHolder
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
                .map(this::parseParams)
                .parallel()
                .flatMap {
                    this.fetch(it)
                            .doOnError { e -> log.warn("Error during native call: ${e.message}") }
                }
                .sequential()
    }

    fun parseParams(it: CallContext<RawCallDetails>): CallContext<ParsedCallDetails> {
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
            result.setPayload(ByteString.copyFrom(it.result))
        }

        return result.build()
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

    fun prepareCall(request: BlockchainOuterClass.NativeCallRequest): Flux<CallContext<RawCallDetails>> {
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

    fun prepareCall(request: BlockchainOuterClass.NativeCallRequest, upstream: Multistream): Flux<CallContext<RawCallDetails>> {
        val chain = Chain.byId(request.chainValue)
        return Flux.fromIterable(request.itemsList).flatMap {
            val method = it.method
            val params = it.payload.toStringUtf8()

            // for ethereum the actual block needed for the call may be specified in the call parameters
            val callSpecificMatcher: Mono<Selector.Matcher> = if (BlockchainType.from(upstream.chain) == BlockchainType.ETHEREUM) {
                ethereumCallSelectors[chain]?.getMatcher(method, params, upstream.getHead())
            } else {
                null
            } ?: Mono.empty()

            callSpecificMatcher.defaultIfEmpty(Selector.empty).map { csm ->
                val matcher = Selector.Builder()
                        .withMatcher(csm)
                        .forMethod(method)
                        .forLabels(Selector.convertToMatcher(request.selector))

                val callQuorum = upstream.getMethods().getQuorumFor(method) ?: AlwaysQuorum() // can be null in tests
                callQuorum.init(upstream.getHead())

                // for NotLaggingQuorum it makes sense to select compatible upstreams before the call
                if (callQuorum is NotLaggingQuorum) {
                    val lag = callQuorum.maxLag
                    val minHeight = ((upstream.getHead().getCurrentHeight() ?: 0) - lag).coerceAtLeast(0)
                    val heightMatcher = Selector.HeightMatcher(minHeight)
                    matcher.withMatcher(heightMatcher)
                }

                CallContext(it.id, upstream, matcher.build(), callQuorum, RawCallDetails(method, params))
            }
        }
    }

    fun fetch(ctx: CallContext<ParsedCallDetails>): Mono<CallResult> {
        return ctx.upstream.getRoutedApi(ctx.matcher)
                .flatMap { api ->
                    api.read(JsonRpcRequest(ctx.payload.method, ctx.payload.params))
                            .flatMap(JsonRpcResponse::requireResult)
                            .map {
                                CallResult.ok(ctx.id, it)
                            }
                }.switchIfEmpty(
                        Mono.just(ctx).flatMap(this::executeOnRemote)
                )
                .onErrorResume {
                    if (it is CallFailure) {
                        Mono.just(CallResult.fail(it.id, it.reason))
                    } else {
                        Mono.just(CallResult.fail(ctx.id, it))
                    }
                }
    }

    fun executeOnRemote(ctx: CallContext<ParsedCallDetails>): Mono<CallResult> {
        if (!ctx.upstream.getMethods().isAllowed(ctx.payload.method)) {
            return Mono.error(RpcException(RpcResponseError.CODE_METHOD_NOT_EXIST, "Unsupported method"))
        }
        val reader = quorumReaderFactory.create(ctx.getApis(), ctx.callQuorum)
        return reader
                .read(JsonRpcRequest(ctx.payload.method, ctx.payload.params))
                .map {
                    CallResult(ctx.id, it.value, null)
                }
                .doOnNext {
                    it.result?.let { value ->
                        ctx.upstream.postprocessor
                                .onReceive(ctx.payload.method, ctx.payload.params, value)
                    }
                }
                .onErrorResume { t ->
                    val failure = if (t is CallFailure) {
                        CallResult.fail(t.id, t.reason)
                    } else {
                        CallResult.fail(ctx.id, t)
                    }
                    Mono.just(failure)
                }
                .switchIfEmpty(
                        Mono.just(CallResult.fail(ctx.id, 1, "No response or no available upstream for ${ctx.payload.method}"))
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

    open class CallContext<T>(val id: Int,
                              val upstream: Multistream,
                              val matcher: Selector.Matcher,
                              val callQuorum: CallQuorum,
                              val payload: T) {
        fun <X> withPayload(payload: X): CallContext<X> {
            return CallContext(id, upstream, matcher, callQuorum, payload)
        }

        fun getApis(): ApiSource {
            return upstream.getApiSource(matcher)
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
                    else -> CallError(1, t.message ?: "Upstream Error", null)
                }
            }
        }
    }

    open class CallResult(val id: Int, val result: ByteArray?, val error: CallError?) {
        companion object {
            fun ok(id: Int, result: ByteArray): CallResult {
                return CallResult(id, result, null)
            }

            fun fail(id: Int, errorCore: Int, errorMessage: String): CallResult {
                return CallResult(id, null, CallError(errorCore, errorMessage, null))
            }

            fun fail(id: Int, error: Throwable): CallResult {
                return CallResult(id, null, CallError.from(error))
            }
        }

        fun isError(): Boolean {
            return error != null
        }
    }

    class RawCallDetails(val method: String, val params: String)
    class ParsedCallDetails(val method: String, val params: List<Any>)
}