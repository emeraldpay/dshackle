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
import io.emeraldpay.dshackle.upstream.CallMethods
import io.emeraldpay.dshackle.upstream.DirectCallMethods
import io.emeraldpay.dshackle.upstream.ethereum.DirectEthereumApi
import io.infinitape.etherjar.rpc.Batch
import io.infinitape.etherjar.rpc.ExecutableBatch
import io.infinitape.etherjar.rpc.RpcCall
import io.infinitape.etherjar.rpc.RpcClient
import io.infinitape.etherjar.rpc.transport.BatchStatus

import java.util.concurrent.CompletableFuture

class EthereumApiStub extends DirectEthereumApi {

    private String id
    private static ObjectMapper objectMapper = TestingCommons.objectMapper()
    private static RpcClient rpcClient = new RpcClientMock();

    EthereumApiStub(Integer id) {
        this(id.toString())
    }

    EthereumApiStub(String id) {
        super(rpcClient, objectMapper, new DirectCallMethods())
        this.id = id
    }

    @Override
    String toString() {
        return "API Stub $id"
    }

    static class RpcClientMock implements RpcClient {
        @Override
        CompletableFuture<BatchStatus> execute(Batch batch) {
            return null
        }

        @Override
        def <RES> CompletableFuture<RES> execute(RpcCall<?, RES> call) {
            return null
        }

        @Override
        ExecutableBatch batch() {
            return null
        }

        @Override
        EthCommands eth() {
            return null
        }

        @Override
        TraceCommands trace() {
            return null
        }
    }
}
