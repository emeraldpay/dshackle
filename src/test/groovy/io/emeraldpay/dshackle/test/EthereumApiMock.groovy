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
package io.emeraldpay.dshackle.test

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.protobuf.ByteString
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.dshackle.upstream.DirectCallMethods
import io.emeraldpay.dshackle.upstream.ethereum.DirectEthereumApi
import io.emeraldpay.grpc.Chain
import io.grpc.stub.StreamObserver
import io.infinitape.etherjar.rpc.ReactorRpcClient
import io.infinitape.etherjar.rpc.RpcClient
import io.infinitape.etherjar.rpc.RpcResponseError
import io.infinitape.etherjar.rpc.json.ResponseJson
import org.jetbrains.annotations.NotNull
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono

class EthereumApiMock extends DirectEthereumApi {

    private static final Logger log = LoggerFactory.getLogger(this)
    List<PredefinedResponse> predefined = []
    private ObjectMapper objectMapper

    EthereumApiMock(@NotNull ReactorRpcClient rpcClient, @NotNull ObjectMapper objectMapper, @NotNull Chain chain) {
        super(rpcClient, objectMapper, new DirectCallMethods())
        this.objectMapper = objectMapper
    }

    EthereumApiMock answerOnce(@NotNull String method, List<Object> params, Object result) {
        return answer(method, params, result, 1)
    }

    EthereumApiMock answer(@NotNull String method, List<Object> params, Object result,
                           Integer limit = null, Throwable exception = null) {
        predefined << new PredefinedResponse(method: method, params: params, result: result, limit: limit, exception: exception)
        return this
    }

    @Override
    Mono<byte[]> execute(int id, @NotNull String method, @NotNull List<?> params) {
        def predefined = predefined.find { it.isSame(id, method, params) }
        ResponseJson json = new ResponseJson<Object, Integer>(id: id)
        if (predefined != null) {
            if (predefined.exception != null) {
                predefined.onCalled()
                predefined.print()
                throw predefined.exception
            }
            json.result = predefined.result
        } else {
            log.error("Method ${method} with ${params} is not mocked")
            json.error = new RpcResponseError(-32601, "Method ${method} with ${params} is not mocked")
        }
        predefined.onCalled()
        predefined.print()
        return Mono.just(objectMapper.writeValueAsBytes(json))
    }

    def nativeCall(BlockchainOuterClass.NativeCallRequest request, StreamObserver<BlockchainOuterClass.NativeCallReplyItem> responseObserver) {
        request.itemsList.forEach { req ->
            def resp = execute(req.id, req.method, objectMapper.readerFor(List).readValue(req.payload.toByteArray()))
            resp.subscribe {
                def proto = BlockchainOuterClass.NativeCallReplyItem.newBuilder()
                        .setId(req.id)
                        .setSucceed(true)
                        .setPayload(ByteString.copyFrom(resp.block()))
                responseObserver.onNext(proto.build())
            }
        }
        responseObserver.onCompleted()
    }

    class PredefinedResponse {
        String method
        List params
        Object result
        Integer limit
        Throwable exception

        boolean isSame(int id, String method, List<?> params) {
            if (limit != null) {
                if (limit <= 0) {
                    return false
                }
            }
            if (method != this.method) {
                return false
            }
            if (this.params == null) {
                return true
            }
            return this.params == params
        }

        void onCalled() {
            if (limit != null) {
                limit--
            }
        }

        void print() {
            println "Execute API: $method ${params ? params : '_'} >> $result"
        }
    }
}
