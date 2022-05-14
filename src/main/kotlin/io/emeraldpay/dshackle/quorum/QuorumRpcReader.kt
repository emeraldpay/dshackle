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
package io.emeraldpay.dshackle.quorum

import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.ApiSource
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcError
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcException
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.etherjar.rpc.RpcException
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.util.function.Tuple2
import reactor.util.function.Tuples
import java.util.function.BiFunction
import java.util.function.Function

/**
 * Makes request with applying Quorum
 */
class QuorumRpcReader(
    private val apiControl: ApiSource,
    private val quorum: CallQuorum
) : Reader<JsonRpcRequest, QuorumRpcReader.Result> {

    companion object {
        private val log = LoggerFactory.getLogger(QuorumRpcReader::class.java)
    }

    override fun read(key: JsonRpcRequest): Mono<Result> {
        // needs at least one response, so start a request
        apiControl.request(1)

        // uses a mix of retry strategy and managed Publisher for calls.
        // retry is used when an error happened
        // but if no error received, we check quorum and if not enough data received we request more
        // eventually source of upstreams is Completed (or something Errored) and if finalizes the result

        val retrySpec = reactor.util.retry.Retry.from { signal ->
            signal.takeUntil {
                it.totalRetries() >= 3 || quorum.isResolved() || quorum.isFailed()
            }.doOnNext {
                // when retried it needs one more API source
                apiControl.request(1)
            }
        }

        val defaultResult: Mono<Result> = setupDefaultResult(key)

        return Flux.from(apiControl)
            .transform(execute(key, retrySpec))
            .next()
            // if last call resulted in error it's still possible that request was resolved correctly. ex. for BroadcastQuorum
            .onErrorResume { err ->
                if (quorum.isResolved()) {
                    Mono.just(quorum)
                } else {
                    Mono.error(err)
                }
            }
            .doOnNext {
                if (!it.isResolved() && !it.isFailed()) {
                    log.debug("No quorum for ${key.method} using [$quorum]. Error: ${it.getError()?.message ?: ""}")
                }
            }
            .transform(processResult(defaultResult))
    }

    fun execute(key: JsonRpcRequest, retrySpec: reactor.util.retry.Retry): Function<Flux<Upstream>, Mono<CallQuorum>> {
        val quorumReduce = BiFunction<CallQuorum, Tuple2<Tuple2<ByteArray, String>, Upstream>, CallQuorum> { res, a ->
            if (res.record(a.t1.t1, a.t1.t2, a.t2)) {
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
                }
                .flatMap { api ->
                    callApi(api, key)
                }
                .retryWhen(retrySpec)
                // record all correct responses until quorum reached
                .reduce(quorum, quorumReduce)
        }
    }

    fun processResult(defaultResult: Mono<Result>): Function<Mono<CallQuorum>, Mono<Result>> {
        return Function { quorumResult ->
            quorumResult
                .filter { it.isResolved() } // return nothing if not resolved
                .map {
                    // TODO find actual quorum number
                    QuorumRpcReader.Result(it.getResult()!!, it.getSignature(), 1)
                }
                .switchIfEmpty(defaultResult)
        }
    }

    fun callApi(api: Upstream, key: JsonRpcRequest): Mono<Tuple2<Tuple2<ByteArray, String>, Upstream>> {
        return api.getApi()
            .read(key)
            .flatMap { response ->
                response.requireResultWithSignature()
                    .onErrorResume { err ->
                        // on error notify quorum, it may use error message or other details
                        val cleanErr: JsonRpcException = when (err) {
                            is RpcException -> JsonRpcException.from(err)
                            is JsonRpcException -> err
                            else -> JsonRpcException(
                                JsonRpcResponse.NumberId(key.id),
                                JsonRpcError(-32603, "Unhandled internal error: ${err.javaClass}")
                            )
                        }
                        quorum.record(cleanErr, response.sig, api)
                        // if it's failed after that, then we don't need more calls, stop api source
                        if (quorum.isFailed()) {
                            apiControl.resolve()
                        } else {
                            apiControl.request(1)
                        }
                        Mono.empty()
                    }
            }
            .map { Tuples.of(it, api) }
    }

    fun setupDefaultResult(key: JsonRpcRequest): Mono<Result> {
        return Mono.just(quorum).flatMap { q ->
            if (q.isFailed()) {
                Mono.error<Result>(
                    q.getError()?.asException(JsonRpcResponse.NumberId(key.id))
                        ?: JsonRpcException(JsonRpcResponse.NumberId(key.id), JsonRpcError(-32603, "Unhandled Upstream error"))
                )
            } else {
                log.warn("Did not get any result from upstream. Method [${key.method}] using [$q]")
                Mono.empty<Result>()
            }
        }
    }

    class Result(
        val value: ByteArray,
        val signature: String,
        val quorum: Int
    )
}
