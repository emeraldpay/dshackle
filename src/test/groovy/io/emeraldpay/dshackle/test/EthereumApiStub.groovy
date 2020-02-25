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
import io.emeraldpay.dshackle.upstream.calls.DirectCallMethods
import io.emeraldpay.dshackle.upstream.ethereum.DirectEthereumApi
import io.infinitape.etherjar.rpc.ReactorBatch
import io.infinitape.etherjar.rpc.ReactorRpcClient
import io.infinitape.etherjar.rpc.RpcCall
import io.infinitape.etherjar.rpc.RpcCallResponse
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

class EthereumApiStub extends DirectEthereumApi {

    private String id
    private static ObjectMapper objectMapper = TestingCommons.objectMapper()
    private static ReactorRpcClient rpcClient = new RpcClientMock();

    EthereumApiStub(Integer id) {
        this(id.toString())
    }

    EthereumApiStub(String id) {
        super(rpcClient, null, objectMapper, new DirectCallMethods())
        this.id = id
    }

    @Override
    String toString() {
        return "API Stub $id"
    }

    static class RpcClientMock implements ReactorRpcClient {

        @Override
        Flux<RpcCallResponse> execute(ReactorBatch batch) {
            return Flux.error(new Exception("Not implemented in mock"))
        }

        @Override
        def <JS, RES> Mono<RES> execute(RpcCall<JS, RES> call) {
            return Mono.error(new Exception("Not implemented in mock"))
        }
    }
}
