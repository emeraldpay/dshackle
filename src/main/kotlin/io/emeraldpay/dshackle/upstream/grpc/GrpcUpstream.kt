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
package io.emeraldpay.dshackle.upstream.grpc

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.BlockchainOuterClass.NativeSubscribeRequest
import io.emeraldpay.api.proto.ReactorBlockchainGrpc
import io.emeraldpay.dshackle.upstream.Upstream
import reactor.core.publisher.Flux

interface GrpcUpstream : Upstream {

    /**
     * Update the configuration of the upstream with the new data.
     * Called on the first creation, and each time a new state received from upstream
     */
    fun update(conf: BlockchainOuterClass.DescribeChain): Boolean

    fun getBlockchainApi(): ReactorBlockchainGrpc.ReactorBlockchainStub

    fun proxySubscribe(request: NativeSubscribeRequest): Flux<out Any>
}
