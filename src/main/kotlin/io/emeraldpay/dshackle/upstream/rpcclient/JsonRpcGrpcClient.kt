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
import io.emeraldpay.api.proto.BlockchainOuterClass.NativeCallReplySignature
import io.emeraldpay.api.proto.ReactorBlockchainGrpc
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.signature.ResponseSigner
import io.emeraldpay.etherjar.rpc.RpcException
import io.emeraldpay.etherjar.rpc.RpcResponseError
import io.grpc.StatusRuntimeException
import org.apache.commons.lang3.time.StopWatch
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import java.util.concurrent.TimeUnit

class JsonRpcGrpcClient(
    private val stub: ReactorBlockchainGrpc.ReactorBlockchainStub,
    private val chain: Chain,
    private val metrics: RpcMetrics?,
) {

    companion object {
        private val log = LoggerFactory.getLogger(JsonRpcGrpcClient::class.java)
    }

    fun getReader(): Reader<JsonRpcRequest, JsonRpcResponse> {
        return Executor(stub, chain, metrics)
    }

    class Executor(
        private val stub: ReactorBlockchainGrpc.ReactorBlockchainStub,
        private val chain: Chain,
        private val metrics: RpcMetrics?
    ) : Reader<JsonRpcRequest, JsonRpcResponse> {

        override fun read(key: JsonRpcRequest): Mono<JsonRpcResponse> {
            val timer = StopWatch()
            val req = BlockchainOuterClass.NativeCallRequest.newBuilder()
                .setChainValue(chain.id)

            key.selector?.let { req.selector = it }

            val reqItem = BlockchainOuterClass.NativeCallItem.newBuilder()
                .setId(1)
                .setMethod(key.method)
                .setPayload(ByteString.copyFrom(Global.objectMapper.writeValueAsBytes(key.params)))
            if (key.nonce != null) {
                reqItem.nonce = key.nonce
            }
            req.addItems(reqItem.build())

            return Mono.just(key)
                .doOnNext { timer.start() }
                .flatMap {
                    stub.nativeCall(req.build())
                        .single()
                        .onErrorResume(::handleError)
                        .flatMap(::handleResponse)
                }
                .doOnNext {
                    if (timer.isStarted) {
                        metrics?.timer?.record(timer.getTime(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS)
                    }
                }
        }

        fun handleResponse(resp: BlockchainOuterClass.NativeCallReplyItem): Mono<JsonRpcResponse> =
            if (resp.succeed) {
                val bytes = resp.payload.toByteArray()
                val signature = if (resp.hasSignature()) {
                    extractSignature(resp.signature)
                } else {
                    null
                }
                Mono.just(JsonRpcResponse(bytes, null, JsonRpcResponse.NumberId(0), signature))
            } else {
                metrics?.fails?.increment()
                Mono.error(
                    RpcException(
                        RpcResponseError.CODE_UPSTREAM_CONNECTION_ERROR,
                        resp.errorMessage
                    )
                )
            }

        fun handleError(t: Throwable): Mono<BlockchainOuterClass.NativeCallReplyItem> {
            metrics?.fails?.increment()
            return when (t) {
                is StatusRuntimeException -> Mono.error(
                    RpcException(
                        RpcResponseError.CODE_UPSTREAM_CONNECTION_ERROR,
                        "Remote status code: ${t.status.code.name}"
                    )
                )

                else -> Mono.error(
                    RpcException(
                        RpcResponseError.CODE_UPSTREAM_CONNECTION_ERROR,
                        "Other connection error"
                    )
                )
            }
        }

        fun extractSignature(resp: NativeCallReplySignature?): ResponseSigner.Signature? {
            if (resp == null || resp.signature == null || resp.signature.isEmpty || resp.upstreamId == null || resp.upstreamId.isEmpty()) {
                return null
            }
            return ResponseSigner.Signature(
                resp.signature.toByteArray(),
                resp.upstreamId,
                resp.keyId
            )
        }
    }
}
