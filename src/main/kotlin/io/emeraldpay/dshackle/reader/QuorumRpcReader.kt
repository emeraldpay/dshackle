/**
 * Copyright (c) 2020 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.reader

import io.emeraldpay.dshackle.quorum.CallQuorum
import io.emeraldpay.dshackle.upstream.ApiSource
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcError
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcException
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.dshackle.upstream.signature.ResponseSigner
import io.emeraldpay.etherjar.rpc.RpcException
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.util.function.Tuple2
import reactor.util.function.Tuple3
import reactor.util.function.Tuples
import java.util.Optional
import java.util.function.BiFunction
import java.util.function.Function

/**
 * Makes request with applying Quorum
 */
class QuorumRpcReader(
    private val apiControl: ApiSource,
    private val quorum: CallQuorum,
    private val signer: ResponseSigner?,
) : Reader<JsonRpcRequest, QuorumRpcReader.Result> {
    companion object {
        private val log = LoggerFactory.getLogger(QuorumRpcReader::class.java)
    }

    constructor(apiControl: ApiSource, quorum: CallQuorum) : this(apiControl, quorum, null)

    override fun read(key: JsonRpcRequest): Mono<Result> {
        // let the quorum know the number of total upstreams available which may affect their requirements for data
        quorum.setTotalUpstreams(apiControl.size)

        // needs at least one response, so start a request
        apiControl.request(1)

        // uses a mix of retry strategy and managed Publisher for calls.
        // retry is used when an error happened
        // but if no error received, we check quorum and if not enough data received we request more
        // eventually source of upstreams is Completed (or something Errored) and if finalizes the result

        val retrySpec =
            reactor.util.retry.Retry.from { signal ->
                signal
                    .takeUntil {
                        it.totalRetries() >= 3 || quorum.isResolved() || quorum.isFailed()
                    }.doOnNext {
                        // when retried it needs one more API source
                        apiControl.request(1)
                    }
            }

        val defaultResult: Mono<Result> = setupDefaultResult(key)

        return Flux
            .from(apiControl)
            .doOnComplete(quorum::close)
            .transform(execute(key, retrySpec))
            .next()
            // if last call resulted in error it's still possible that request was resolved correctly. ex. for BroadcastQuorum
            .onErrorResume { err ->
                if (quorum.isResolved()) {
                    Mono.just(quorum)
                } else {
                    Mono.error(err)
                }
            }.doOnNext {
                if (!it.isResolved() && !it.isFailed()) {
                    log.debug("No quorum for ${key.method} using [$quorum]. Error: ${it.getError()?.message ?: ""}")
                }
            }.transform(processResult(defaultResult))
    }

    fun execute(
        key: JsonRpcRequest,
        retrySpec: reactor.util.retry.Retry,
    ): Function<Flux<Upstream>, Mono<CallQuorum>> {
        val quorumReduce =
            BiFunction<CallQuorum, Tuple3<ByteArray, Optional<ResponseSigner.Signature>, Upstream>, CallQuorum> { res, a ->
                if (res.record(a.t1, a.t2.orElse(null), a.t3)) {
                    apiControl.resolve()
                } else {
                    // quorum needs more responses, so ask api controller to make another
                    apiControl.request(1)
                }
                res
            }
        return Function { apiFlux ->
            apiFlux
                .takeUntil {
                    quorum.isFailed() || quorum.isResolved()
                }.flatMap { api ->
                    callApi(api, key)
                }.retryWhen(retrySpec)
                // record all correct responses until quorum reached
                .reduce(quorum, quorumReduce)
        }
    }

    fun processResult(defaultResult: Mono<Result>): Function<Mono<CallQuorum>, Mono<Result>> =
        Function { quorumResult ->
            quorumResult
                .filter { it.isResolved() } // return nothing if not resolved
                .map {
                    // TODO find actual quorum number
                    Result(it.getResult()!!, it.getSignature(), 1)
                }.switchIfEmpty(defaultResult)
        }

    fun callApi(
        api: Upstream,
        key: JsonRpcRequest,
    ): Mono<Tuple3<ByteArray, Optional<ResponseSigner.Signature>, Upstream>> =
        api
            .getIngressReader()
            .read(key)
            .flatMap { response ->
                response
                    .requireResult()
                    .transform(withSignature(api, key, response))
            }
            // must catch not only the processing of a response but also errors thrown from the .read() call
            .transform(withErrorResume(api, key))
            .map { Tuples.of(it.t1, it.t2, api) }

    fun withSignature(
        api: Upstream,
        key: JsonRpcRequest,
        response: JsonRpcResponse,
    ): Function<Mono<ByteArray>, Mono<Tuple2<ByteArray, Optional<ResponseSigner.Signature>>>> =
        Function { src ->
            src.map {
                val signature =
                    response.providedSignature
                        ?: if (key.nonce != null) {
                            signer?.sign(key.nonce, response.resultOrEmpty, api)
                        } else {
                            null
                        }
                Tuples.of(it, Optional.ofNullable(signature))
            }
        }

    fun <T> withErrorResume(
        api: Upstream,
        key: JsonRpcRequest,
    ): Function<Mono<T>, Mono<T>> =
        Function { src ->
            src.onErrorResume { err -> handleError<T>(err, key, api) }
        }

    fun <T> handleError(
        err: Throwable,
        key: JsonRpcRequest,
        api: Upstream,
    ): Mono<T> {
        // when the call failed with an error we want to notify the quorum because
        // it may use the error message or other details
        val cleanErr: JsonRpcException =
            when (err) {
                is RpcException -> JsonRpcException.from(err)
                is JsonRpcException -> err
                else -> {
                    // if that happened something is really wrong, all errors must be caught and be provided as RpcException
                    log.error("Internal error propagate to the caller", err)
                    JsonRpcException(
                        JsonRpcResponse.NumberId(key.id),
                        JsonRpcError(-32603, "Unhandled internal error: ${err.javaClass} ${err.message}"),
                    )
                }
            }
        if (CallQuorum.isConnectionUnavailable(cleanErr)) {
            // If the upstream is temporary unavailable we should try to go with other upstreams when they are available
            // Note that the error may not be fully recorded in this case, because of the special handler in AlwaysQuorum
            // that ignores this kind of error
            apiControl.exclude(api)
        }
        quorum.record(cleanErr, null, api)

        // if it's failed after that, then we don't need more calls, stop api source
        if (quorum.isFailed()) {
            apiControl.resolve()
        } else {
            apiControl.request(1)
        }
        return Mono.empty()
    }

    fun setupDefaultResult(key: JsonRpcRequest): Mono<Result> =
        Mono.just(quorum).flatMap { q ->
            if (q.isFailed()) {
                Mono.error<Result>(
                    q.getError()?.asException(JsonRpcResponse.NumberId(key.id))
                        ?: JsonRpcException(JsonRpcResponse.NumberId(key.id), JsonRpcError(-32603, "Unhandled Upstream error")),
                )
            } else {
                log.warn("Did not get any result from upstream. Method [${key.method}] using [$q]")
                Mono.empty<Result>()
            }
        }

    class Result(
        val value: ByteArray,
        val signature: ResponseSigner.Signature?,
        val quorum: Int,
    )
}
