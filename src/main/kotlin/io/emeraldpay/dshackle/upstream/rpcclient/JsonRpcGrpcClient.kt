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
import io.emeraldpay.dshackle.reader.ChainReader
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.ChainResponse
import io.emeraldpay.dshackle.upstream.RequestMetrics
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.ethereum.rpc.RpcException
import io.emeraldpay.dshackle.upstream.ethereum.rpc.RpcResponseError
import io.emeraldpay.dshackle.upstream.signature.ResponseSigner
import io.grpc.StatusRuntimeException
import org.apache.commons.lang3.time.StopWatch
import reactor.core.publisher.Mono
import java.util.concurrent.TimeUnit

class JsonRpcGrpcClient(
    private val stub: ReactorBlockchainGrpc.ReactorBlockchainStub,
    private val chain: Chain,
    private val metrics: RequestMetrics?,
) {

    fun getReader(): ChainReader {
        return Executor(stub, chain, metrics)
    }

    class Executor(
        private val stub: ReactorBlockchainGrpc.ReactorBlockchainStub,
        private val chain: Chain,
        private val metrics: RequestMetrics?,
    ) : ChainReader {

        override fun read(key: ChainRequest): Mono<ChainResponse> {
            val timer = StopWatch()
            val req = BlockchainOuterClass.NativeCallRequest.newBuilder()
                .setChainValue(chain.id)

            key.selector?.let { req.selector = it }

            val reqItem = BlockchainOuterClass.NativeCallItem.newBuilder()
                .setId(1)
                .setMethod(key.method)
            if (key.params is RestParams) {
                reqItem.setRestData(
                    BlockchainOuterClass.RestData.newBuilder()
                        .addAllHeaders(mapKeyValue(key.params.headers))
                        .addAllQueryParams(mapKeyValue(key.params.queryParams))
                        .setPayload(ByteString.copyFrom(Global.objectMapper.writeValueAsBytes(key.params.payload)))
                        .addAllPathParams(key.params.pathParams)
                        .build(),
                )
            } else {
                reqItem.setPayload(
                    ByteString.copyFrom(
                        Global.objectMapper.writeValueAsBytes(
                            when (key.params) {
                                is ListParams -> key.params.list
                                is ObjectParams -> key.params.obj
                                else -> throw IllegalStateException("Wrong param types ${key.params.javaClass}")
                            },
                        ),
                    ),
                )
            }
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

        fun handleResponse(resp: BlockchainOuterClass.NativeCallReplyItem): Mono<ChainResponse> =
            if (resp.succeed) {
                val bytes = resp.payload.toByteArray()
                val signature = if (resp.hasSignature()) {
                    extractSignature(resp.signature)
                } else {
                    null
                }
                Mono.just(ChainResponse(bytes, null, ChainResponse.NumberId(0), null, signature, Upstream.UpstreamSettingsData(0, resp.upstreamId, resp.upstreamNodeVersion)))
            } else {
                metrics?.fails?.increment()
                Mono.error(
                    RpcException(
                        resp.errorCode,
                        resp.errorMessage,
                        if (resp.errorData == null || resp.errorData.isEmpty()) null else resp.errorData,
                    ),
                )
            }

        fun handleError(t: Throwable): Mono<BlockchainOuterClass.NativeCallReplyItem> {
            metrics?.fails?.increment()
            return when (t) {
                is StatusRuntimeException -> Mono.error(
                    RpcException(
                        RpcResponseError.CODE_UPSTREAM_CONNECTION_ERROR,
                        "Remote status code: ${t.status.code.name}",
                    ),
                )

                else -> Mono.error(
                    RpcException(
                        RpcResponseError.CODE_UPSTREAM_CONNECTION_ERROR,
                        "Other connection error",
                    ),
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
                resp.keyId,
            )
        }

        private fun mapKeyValue(entries: List<Pair<String, String>>) =
            entries.map {
                BlockchainOuterClass.KeyValue
                    .newBuilder()
                    .setKey(it.first)
                    .setValue(it.second)
                    .build()
            }
    }
}
