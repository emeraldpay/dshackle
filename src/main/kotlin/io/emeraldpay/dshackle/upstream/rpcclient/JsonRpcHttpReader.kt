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
package io.emeraldpay.dshackle.upstream.rpcclient

import io.emeraldpay.dshackle.config.AuthConfig
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.ChainResponse
import io.emeraldpay.dshackle.upstream.HttpReader
import io.emeraldpay.dshackle.upstream.RequestMetrics
import io.emeraldpay.dshackle.upstream.ethereum.rpc.RpcResponseError
import io.emeraldpay.dshackle.upstream.rpcclient.stream.JsonRpcStreamParser
import io.emeraldpay.dshackle.upstream.stream.AggregateResponse
import io.emeraldpay.dshackle.upstream.stream.Response
import io.emeraldpay.dshackle.upstream.stream.SingleResponse
import io.emeraldpay.dshackle.upstream.stream.StreamResponse
import io.netty.buffer.Unpooled
import org.apache.commons.lang3.time.StopWatch
import reactor.core.publisher.Mono
import java.util.concurrent.TimeUnit
import java.util.function.Function

/**
 * JSON RPC client
 */
class JsonRpcHttpReader(
    target: String,
    metrics: RequestMetrics,
    basicAuth: AuthConfig.ClientBasicAuth? = null,
    tlsCAAuth: ByteArray? = null,
) : HttpReader(target, metrics, basicAuth, tlsCAAuth) {

    private val parser = ResponseRpcParser()
    private val streamParser = JsonRpcStreamParser()

    private fun execute(request: ChainRequest): Mono<out Response> {
        val bytesRequest = request.toJson()

        val response = httpClient
            .post()
            .uri(target)
            .send(Mono.just(Unpooled.wrappedBuffer(bytesRequest)))

        return if (!request.isStreamed) {
            response.response { header, bytes ->
                val statusCode = header.status().code()

                bytes.aggregate().asByteArray().map {
                    AggregateResponse(it, statusCode)
                }
            }.single()
        } else {
            response.responseConnection { t, u ->
                streamParser.streamParse(
                    t.status().code(),
                    u.inbound().receive().asByteArray(),
                )
            }.single()
        }
    }

    override fun internalRead(key: ChainRequest): Mono<ChainResponse> {
        val startTime = StopWatch()
        return Mono.just(key)
            .doOnNext {
                if (!startTime.isStarted) {
                    startTime.start()
                }
            }
            .flatMap(this@JsonRpcHttpReader::execute)
            .doOnNext {
                if (startTime.isStarted) {
                    metrics.timer.record(startTime.nanoTime, TimeUnit.NANOSECONDS)
                }
            }
            .transform(asJsonRpcResponse(key))
    }

    /**
     * Process response from the upstream and convert it to JsonRpcResponse.
     * The input is a pair of (Http Status Code, Http Response Body)
     */
    private fun asJsonRpcResponse(key: ChainRequest): Function<Mono<out Response>, Mono<ChainResponse>> {
        return Function { resp ->
            resp.map {
                when (it) {
                    is AggregateResponse -> {
                        val parsed = parser.parse(it.response)
                        val statusCode = it.code
                        if (statusCode != 200) {
                            if (parsed.hasError() && parsed.error!!.code != RpcResponseError.CODE_UPSTREAM_INVALID_RESPONSE) {
                                // extracted the error details from the HTTP Body
                                parsed
                            } else {
                                // here we got a valid response with ERROR as HTTP Status Code. We assume that HTTP Status has
                                // a higher priority so return an error here anyway
                                ChainResponse.error(
                                    RpcResponseError.CODE_UPSTREAM_INVALID_RESPONSE,
                                    "HTTP Code: $statusCode",
                                    ChainResponse.NumberId(key.id),
                                )
                            }
                        } else {
                            parsed
                        }
                    }
                    is StreamResponse -> {
                        ChainResponse(it.stream, key.id)
                    }
                    is SingleResponse -> {
                        if (it.hasError()) {
                            ChainResponse(null, it.error)
                        } else {
                            ChainResponse(it.result, null)
                        }
                    }
                }
            }
        }
    }
}
