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

import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import reactor.core.publisher.Mono

class EthereumApiStub implements Reader<JsonRpcRequest, JsonRpcResponse> {

    private String id

    EthereumApiStub(Integer id) {
        this(id.toString())
    }

    EthereumApiStub(String id) {
        this.id = id
    }

    @Override
    String toString() {
        return "API Stub $id"
    }

    @Override
    Mono<JsonRpcResponse> read(JsonRpcRequest key) {
        return Mono.error(new Exception("Not implemented in mock"))
    }

}
