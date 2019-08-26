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

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.protobuf.ByteString
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.dshackle.upstream.*
import io.emeraldpay.dshackle.upstream.ethereum.DirectEthereumApi
import io.emeraldpay.dshackle.quorum.AlwaysQuorum
import io.emeraldpay.dshackle.quorum.CallQuorum
import io.emeraldpay.grpc.Chain
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.toFlux
import reactor.core.publisher.toMono
import reactor.util.function.Tuples
import java.lang.Exception
import java.time.Duration
import java.util.function.Predicate

@Service
class NativeCall(
        @Autowired private val upstreams: Upstreams,
        @Autowired private val objectMapper: ObjectMapper
) {

    private val log = LoggerFactory.getLogger(NativeCall::class.java)

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
            return Flux.error(CallFailure(0, Exception("Invalid chain id: ${request.chain.number}")))
        }
        val upstream = upstreams.getUpstream(chain)
                ?: return Flux.error(CallFailure(0, Exception("Chain ${chain.id} is unavailable")))

        return prepareCall(request, upstream)
    }

    fun prepareCall(request: BlockchainOuterClass.NativeCallRequest, upstream: AggregatedUpstream): Flux<CallContext<RawCallDetails>> {
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
        return fetchFromCache(ctx)
                .switchIfEmpty(
                        Mono.just(ctx).flatMap(this::executeOnRemote)
                )
    }

    fun fetchFromCache(ctx: CallContext<ParsedCallDetails>): Mono<CallContext<ByteArray>> {
        val cachingApi = ctx.upstream.cache
        return cachingApi.execute(ctx.id, ctx.payload.method, ctx.payload.params).map { ctx.withPayload(it) }
    }

    fun executeOnRemote(ctx: CallContext<ParsedCallDetails>): Mono<CallContext<ByteArray>> {
        val p: Predicate<Any> = CallQuorum.untilResolved(ctx.callQuorum)
        val all = ctx.getApis().toFlux().share()
        //execute on the first API immediately, and then make a delay between each call to not dos upstreams
        val immediate = Flux.from(all).take(1)
        val retries = Flux.from(all).delayElements(Duration.ofMillis(200))
        return Flux.concat(immediate, retries)
                .takeWhile(p)
                .flatMap { api ->
                    api.execute(ctx.id, ctx.payload.method, ctx.payload.params).map { Tuples.of(it, api.upstream!!) }
                }
                .reduce(ctx.callQuorum, CallQuorum.asReducer())
                .filter { it.isResolved() }
                .map {
                    val result  = it.getResult()
                            ?: throw CallFailure(ctx.id, Exception("No response from upstream for ${ctx.payload.method}"))
                    ctx.withPayload(result)
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
                              val upstream: AggregatedUpstream,
                              val matcher: Selector.Matcher,
                              val callQuorum: CallQuorum,
                              val payload: T) {
        fun <X> withPayload(payload: X): CallContext<X> {
            return CallContext(id, upstream, matcher, callQuorum, payload)
        }

        fun getApis(): Iterator<DirectEthereumApi> {
            return upstream.getApis(matcher)
        }
    }

    open class CallFailure(val id: Int, val reason: Throwable): Exception("Failed to call $id: ${reason.message}")

    class RawCallDetails(val method: String, val params: String)
    class ParsedCallDetails(val method: String, val params: List<Any>)
}