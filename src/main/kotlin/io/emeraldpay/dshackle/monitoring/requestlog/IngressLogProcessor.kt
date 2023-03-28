/**
 * Copyright (c) 2022 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.monitoring.requestlog

import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.monitoring.Channel
import io.emeraldpay.dshackle.monitoring.record.RequestRecord
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcException
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import reactor.util.context.ContextView
import java.util.function.Function

class IngressLogProcessor(
    private val writer: RequestLogWriter
) {

    companion object {
        private val log = LoggerFactory.getLogger(IngressLogProcessor::class.java)
    }

    var context = Global.monitoring.ingress

    fun onComplete(upstreamId: String, channel: Channel): (key: JsonRpcRequest) -> Function<Mono<JsonRpcResponse>, Mono<JsonRpcResponse>> {
        return {
            successComplete(upstreamId, channel)
                .andThen(errorComplete(upstreamId, channel))
                .andThen(prepare())
        }
    }

    fun prepare(): Function<Mono<JsonRpcResponse>, Mono<JsonRpcResponse>> {
        return Function { reader ->
            reader.contextWrite(context.prepareForRpcCall())
        }
    }

    fun successComplete(upstreamId: String, channel: Channel): Function<Mono<JsonRpcResponse>, Mono<JsonRpcResponse>> {
        return Function { reader ->
            reader
                .contextWrite(context.cleanup())
                .flatMap { resp ->
                    Mono.deferContextual { ctx ->
                        val event = context.getOrCreate(ctx)
                            .let(copyUpstream(upstreamId, channel))
                            .let(copyEgressId(ctx))
                            .let(copyReqId(ctx))
                            .let {
                                if (resp.hasError()) {
                                    it.copy(
                                        error = RequestRecord.ErrorDetails(
                                            resp.error!!.code,
                                            resp.error.message
                                        )
                                    )
                                } else {
                                    it.copy(responseSize = resp.resultOrEmpty.size)
                                }
                            }
                            .build()
                        writer.accept(event)
                        Mono.just(resp)
                    }
                }
        }
    }

    fun errorComplete(upstreamId: String, channel: Channel): Function<Mono<JsonRpcResponse>, Mono<JsonRpcResponse>> {
        return Function { reader ->
            Mono.deferContextual { ctx ->
                reader
                    .doOnError { t ->
                        if (context.isAvailable(ctx)) {
                            val event = context.getOrCreate(ctx)
                                .let(copyUpstream(upstreamId, channel))
                                .let(copyEgressId(ctx))
                                .let(copyReqId(ctx))
                                .let {
                                    val error = if (t is JsonRpcException) {
                                        RequestRecord.ErrorDetails(
                                            t.error.code,
                                            t.error.message
                                        )
                                    } else {
                                        RequestRecord.ErrorDetails(
                                            0,
                                            "ERROR ${t.javaClass}: ${t.message}"
                                        )
                                    }
                                    it.copy(error = error)
                                }
                                .build()
                            writer.accept(event)
                        }
                    }
            }
        }
    }

    private fun copyUpstream(upstreamId: String, channel: Channel): (RequestRecord.Builder) -> RequestRecord.Builder = {
        it
            .copy(upstreamId = upstreamId, channel = channel)
            .copy(type = RequestType.JSONRPC)
    }

    private fun copyEgressId(ctx: ContextView): (RequestRecord.Builder) -> RequestRecord.Builder = {
        Global.monitoring.egress.getRequest(ctx).let { req ->
            it.copy(ts = req.ts, requestId = req.id)
        }
    }

    private fun copyReqId(ctx: ContextView): (RequestRecord.Builder) -> RequestRecord.Builder = {
        it.copy(rpc = it.rpc.copy(id = context.getRpcId(ctx)))
    }
}
