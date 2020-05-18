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
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.SilentException
import io.emeraldpay.dshackle.upstream.*
import io.emeraldpay.dshackle.quorum.AlwaysQuorum
import io.emeraldpay.dshackle.quorum.CallQuorum
import io.emeraldpay.dshackle.quorum.QuorumReaderFactory
import io.emeraldpay.dshackle.quorum.QuorumRpcReader
import io.emeraldpay.dshackle.upstream.ethereum.EthereumMultistream
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.rpc.RpcException
import io.infinitape.etherjar.rpc.RpcResponseError
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.*
import reactor.util.function.Tuples
import java.lang.Exception

@Service
open class NativeCall(
        @Autowired private val multistreamHolder: MultistreamHolder
) {

    private val log = LoggerFactory.getLogger(NativeCall::class.java)
    private val objectMapper: ObjectMapper = Global.objectMapper

    var quorumReaderFactory: QuorumReaderFactory = QuorumReaderFactory.default()

    open fun nativeCall(requestMono: Mono<BlockchainOuterClass.NativeCallRequest>): Flux<BlockchainOuterClass.NativeCallReplyItem> {
        return requestMono.flatMapMany(this::prepareCall)
                .map(this::setupCallParams)
                .parallel()
                .flatMap(this::fetch)
                .sequential()
                .map(this::buildResponse)
                .doOnError { e -> log.warn("Error during native call: ${e.message}") }
                .onErrorResume(this::processException)
    }

    fun setupCallParams(it: CallContext<RawCallDetails>): CallContext<ParsedCallDetails> {
        val params = extractParams(it.payload.params)
        return it.withPayload(ParsedCallDetails(it.payload.method, params))
    }

    fun buildResponse(it: CallContext<ByteArray>): BlockchainOuterClass.NativeCallReplyItem {
        return BlockchainOuterClass.NativeCallReplyItem.newBuilder()
                .setSucceed(true)
                .setId(it.id)
                .setPayload(ByteString.copyFrom(it.payload))
                .build()
    }

    fun processException(it: Throwable?): Mono<BlockchainOuterClass.NativeCallReplyItem> {
        val id: Int = if (it != null && CallFailure::class.isInstance(it)) {
            (it as CallFailure).id
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
        return request.itemsList.toFlux().map {
            val method = it.method
            val params = it.payload.toStringUtf8()

            val matcher = Selector.Builder()
                    .forMethod(method)
                    .forLabels(Selector.convertToMatcher(request.selector))
                    .build()

            val callQuorum = upstream.getMethods().getQuorumFor(method) ?: AlwaysQuorum()
            callQuorum.init(upstream.getHead())

            CallContext(it.id, upstream, matcher, callQuorum, RawCallDetails(method, params))
        }
    }

    fun fetch(ctx: CallContext<ParsedCallDetails>): Mono<CallContext<ByteArray>> {
        return ctx.upstream.getRoutedApi(ctx.matcher)
                .flatMap { api ->
                    api.read(JsonRpcRequest(ctx.payload.method, ctx.payload.params))
                            .flatMap(JsonRpcResponse::requireResult)
                            .map {
                                ctx.withPayload(it)
                            }
                }.switchIfEmpty(
                        Mono.just(ctx).flatMap(this::executeOnRemote)
                )
                .onErrorMap {
                    CallFailure(ctx.id, it)
                }
    }

    fun executeOnRemote(ctx: CallContext<ParsedCallDetails>): Mono<CallContext<ByteArray>> {
        if (!ctx.upstream.getMethods().isAllowed(ctx.payload.method)) {
            return Mono.error(RpcException(RpcResponseError.CODE_METHOD_NOT_EXIST, "Unsupported method"))
        }
        val reader = quorumReaderFactory.create(ctx.getApis(), ctx.callQuorum)
        return reader.read(JsonRpcRequest(ctx.payload.method, ctx.payload.params))
                .map {
                    ctx.withPayload(it.value)
                }
                .onErrorMap {
                    log.error("Failed to make a call", it)
                    if (it is CallFailure) it
                    else CallFailure(ctx.id, it)
                }
                .switchIfEmpty(
                        Mono.error(CallFailure(ctx.id, Exception("No response or no available upstream for ${ctx.payload.method}")) as Throwable)
                )
    }

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

    open class CallFailure(val id: Int, val reason: Throwable): Exception("Failed to call $id: ${reason.message}")

    class RawCallDetails(val method: String, val params: String)
    class ParsedCallDetails(val method: String, val params: List<Any>)
}