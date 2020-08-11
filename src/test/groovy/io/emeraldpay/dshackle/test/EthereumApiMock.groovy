/**
 * Copyright (c) 2019 ETCDEV GmbH
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
package io.emeraldpay.dshackle.test

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.protobuf.ByteString
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcError
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.grpc.stub.StreamObserver
import io.infinitape.etherjar.rpc.RpcResponseError
import io.infinitape.etherjar.rpc.json.ResponseJson
import org.jetbrains.annotations.NotNull
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono

import java.time.Duration
import java.util.concurrent.Callable

class EthereumApiMock implements Reader<JsonRpcRequest, JsonRpcResponse> {

    private static final Logger log = LoggerFactory.getLogger(this)
    List<PredefinedResponse> predefined = []
    private final ObjectMapper objectMapper = Global.objectMapper

    String id = "default"

    EthereumApiMock() {
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
    Mono<JsonRpcResponse> read(JsonRpcRequest request) {
        Callable<JsonRpcResponse> call = {
            def predefined = predefined.find { it.isSame(request.method, request.params) }
            byte[] result = null
            JsonRpcError error = null
            if (predefined != null) {
                if (predefined.exception != null) {
                    predefined.onCalled()
                    predefined.print()
                    throw predefined.exception
                }
                if (predefined.result instanceof RpcResponseError) {
                    ((RpcResponseError) predefined.result).with { err ->
                        error = new JsonRpcError(err.code, err.message)
                    }
                } else {
//                    ResponseJson json = new ResponseJson<Object, Integer>(id: 1, result: predefined.result)
                    result = objectMapper.writeValueAsBytes(predefined.result)
                }
                predefined.onCalled()
                predefined.print()
            } else {
                log.error("Method ${request.method} with ${request.params} is not mocked")
                error = new JsonRpcError(-32601, "Method ${request.method} with ${request.params} is not mocked")
            }
            return new JsonRpcResponse(result, error)
        } as Callable<JsonRpcResponse>
        return Mono.fromCallable(call)
    }

    def nativeCall(BlockchainOuterClass.NativeCallRequest request, StreamObserver<BlockchainOuterClass.NativeCallReplyItem> responseObserver) {
        request.itemsList.forEach { req ->
            JsonRpcResponse resp = read(new JsonRpcRequest(req.method, objectMapper.readerFor(List).readValue(req.payload.toByteArray())))
                    .block(Duration.ofSeconds(5))
            def proto = BlockchainOuterClass.NativeCallReplyItem.newBuilder()
                    .setId(req.id)
                    .setSucceed(resp.hasResult())
                    .setPayload(ByteString.copyFrom(resp.getResult()))

            resp.error?.with { err ->
                proto.setErrorMessage(err.message)
            }
            responseObserver.onNext(proto.build())
        }
        responseObserver.onCompleted()
    }

    class PredefinedResponse {
        String method
        List params
        Object result
        Integer limit
        Throwable exception

        boolean isSame(String method, List<?> params) {
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
