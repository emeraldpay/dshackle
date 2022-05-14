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

import com.google.protobuf.ByteString
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.ReactorBlockchainGrpc
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.etherjar.rpc.RpcException
import io.emeraldpay.etherjar.rpc.RpcResponseError
import io.emeraldpay.grpc.Chain
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import java.util.concurrent.TimeUnit

class JsonRpcGrpcClient(
    private val stub: ReactorBlockchainGrpc.ReactorBlockchainStub,
    private val chain: Chain,
    private val metrics: RpcMetrics
) {

    companion object {
        private val log = LoggerFactory.getLogger(JsonRpcGrpcClient::class.java)
    }

    fun forSelector(matcher: Selector.Matcher): Reader<JsonRpcRequest, JsonRpcResponse> {
        return Executor(stub, chain, matcher, metrics)
    }

    class Executor(
        private val stub: ReactorBlockchainGrpc.ReactorBlockchainStub,
        private val chain: Chain,
        private val matcher: Selector.Matcher,
        private val metrics: RpcMetrics
    ) : Reader<JsonRpcRequest, JsonRpcResponse> {

        override fun read(key: JsonRpcRequest): Mono<JsonRpcResponse> {
            var startTime: Long = 0
            val req = BlockchainOuterClass.NativeCallRequest.newBuilder()
                .setChainValue(chain.id)

            if (matcher != Selector.empty) {
                Selector.extractLabels(matcher)?.asProto().let {
                    req.setSelector(it)
                }
            }

            BlockchainOuterClass.NativeCallItem.newBuilder()
                .setId(1)
                .setNonce(key.nonce)
                .setMethod(key.method)
                .setPayload(ByteString.copyFrom(Global.objectMapper.writeValueAsBytes(key.params)))
                .build().let {
                    req.addItems(it)
                }

            return Mono.just(key)
                .doOnNext {
                    startTime = System.nanoTime()
                }.flatMap {
                    stub.nativeCall(req.build())
                        .single()
                        .flatMap { resp ->
                            if (resp.succeed) {
                                val bytes = resp.payload.toByteArray()
                                Mono.just(JsonRpcResponse(bytes, null, JsonRpcResponse.NumberId(0), resp.signature))
                            } else {
                                metrics.fails.increment()
                                Mono.error(
                                    RpcException(
                                        RpcResponseError.CODE_UPSTREAM_CONNECTION_ERROR,
                                        resp.errorMessage
                                    )
                                )
                            }
                        }
                }
                .doOnNext {
                    if (startTime > 0) {
                        val now = System.nanoTime()
                        metrics.timer.record(now - startTime, TimeUnit.NANOSECONDS)
                    }
                }
        }
    }
}
