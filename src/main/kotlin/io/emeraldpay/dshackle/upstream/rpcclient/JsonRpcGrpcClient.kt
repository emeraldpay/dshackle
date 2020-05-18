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

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.protobuf.ByteString
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.ReactorBlockchainGrpc
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.grpc.Chain
import io.grpc.Channel
import io.infinitape.etherjar.rpc.RpcException
import io.infinitape.etherjar.rpc.RpcResponseError
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono

class JsonRpcGrpcClient(
        private val stub: ReactorBlockchainGrpc.ReactorBlockchainStub,
        private val chain: Chain
) {

    companion object {
        private val log = LoggerFactory.getLogger(JsonRpcGrpcClient::class.java)
    }

    fun forSelector(matcher: Selector.Matcher): Reader<JsonRpcRequest, JsonRpcResponse> {
        return Executor(stub, chain, matcher)
    }

    class Executor(
            private val stub: ReactorBlockchainGrpc.ReactorBlockchainStub,
            private val chain: Chain,
            private val matcher: Selector.Matcher
    ) : Reader<JsonRpcRequest, JsonRpcResponse> {

        private val parser = JsonRpcParser()

        override fun read(key: JsonRpcRequest): Mono<JsonRpcResponse> {
            val req = BlockchainOuterClass.NativeCallRequest.newBuilder()
                    .setChainValue(chain.id)

            if (matcher != Selector.empty) {
                Selector.extractLabels(matcher)?.asProto().let {
                    req.setSelector(it)
                }
            }

            BlockchainOuterClass.NativeCallItem.newBuilder()
                    .setId(1)
                    .setMethod(key.method)
                    .setPayload(ByteString.copyFrom(Global.objectMapper.writeValueAsBytes(key.params)))
                    .build().let {
                        req.addItems(it)
                    }

            return stub.nativeCall(req.build())
                    .single()
                    .flatMap { resp ->
                        if (resp.succeed) {
                            val bytes = resp.payload.toByteArray()
                            Mono.just(JsonRpcResponse(bytes, null))
                        } else {
                            Mono.error(RpcException(RpcResponseError.CODE_UPSTREAM_CONNECTION_ERROR, resp.errorMessage))
                        }
                    }
        }

    }

}