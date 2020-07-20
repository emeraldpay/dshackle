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
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.infinitape.etherjar.rpc.RpcException
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.util.function.Tuples

/**
 * Makes request with applying Quorum
 */
class QuorumRpcReader(
        private val apis: ApiSource,
        private val quorum: CallQuorum
) : Reader<JsonRpcRequest, QuorumRpcReader.Result> {

    companion object {
        private val log = LoggerFactory.getLogger(QuorumRpcReader::class.java)
    }

    override fun read(key: JsonRpcRequest): Mono<QuorumRpcReader.Result> {
        apis.request(1)

        // uses a mix of retry strategy and managed Publisher for calls.
        // retry is used when an error happened
        // but if no error received, we check quorum and if not enough data received we request more
        // eventually source of upstreams is Completed (or something Errored) and if finalizes the result

        val retrySpec = reactor.util.retry.Retry.from { signal ->
            signal.takeUntil {
                it.totalRetries() >= 3 || quorum.isResolved() || quorum.isFailed()
            }.doOnNext {
                // need one more API source if retried
                apis.request(1)
            }
        }

        val defaultResult: Mono<Result> = Mono.just(quorum).flatMap { q ->
            if (q.isFailed()) {
                //TODO record and return actual error details
                Mono.error<Result>(RpcException(-32000, "Upstream error"))
            } else {
                log.warn("Empty result for ${key.method} as ${q}")
                Mono.empty<Result>()
            }
        }

        return Flux.from(apis)
                .takeUntil {
                    quorum.isFailed() || quorum.isResolved()
                }
                .flatMap { api ->
                    api.getApi()
                            .read(key)
                            .flatMap { response ->
                                response.requireResult()
                                        .onErrorResume { err ->
                                            if (err is RpcException) {
                                                // on error notify quorum, it may use error message or other details
                                                quorum.record(err, api)
                                                // it it's failed after that, then we don't need more calls, stop api source
                                                if (quorum.isFailed()) {
                                                    apis.resolve()
                                                } else {
                                                    apis.request(1)
                                                }
                                            } else {
                                                log.warn("Result processing error", err)
                                            }
                                            Mono.empty()
                                        }
                            }
                            .map { Tuples.of(it, api) }
                }
                .retryWhen(retrySpec)
                // record all correct responses until quorum reached
                .reduce(quorum, { res, a ->
                    if (res.record(a.t1, a.t2)) {
                        apis.resolve()
                    } else {
                        apis.request(1)
                    }
                    res
                })
                // if last call resulted in error it's still possible that request was resolved correctly. i.e. for BroadcastQuorum
                .onErrorResume { err ->
                    if (quorum.isResolved()) {
                        Mono.just(quorum)
                    } else {
                        Mono.error(err)
                    }
                }
                .doOnNext {
                    if (!it.isResolved()) {
                        log.debug("No quorum for ${key.method} as ${quorum}")
                    }
                }
                // return nothing if not resolved
                .filter { it.isResolved() }
                .map {
                    // TODO find actual quorum number
                    QuorumRpcReader.Result(it.getResult()!!, 1)
                }
                .switchIfEmpty(defaultResult)
    }


    class Result(
            val value: ByteArray,
            val quorum: Int
    )
}