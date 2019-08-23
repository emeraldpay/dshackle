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
package io.emeraldpay.dshackle.rpc

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.api.proto.ReactorBlockchainGrpc
import io.grpc.stub.StreamObserver
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@Service
class BlockchainRpc(
        @Autowired private val nativeCall: NativeCall,
        @Autowired private val streamHead: StreamHead,
        @Autowired private val trackTx: TrackTx,
        @Autowired private val trackAddress: TrackAddress,
        @Autowired private val describe: Describe,
        @Autowired private val subscribeStatus: SubscribeStatus
): ReactorBlockchainGrpc.BlockchainImplBase() {

    private val log = LoggerFactory.getLogger(BlockchainRpc::class.java)

    override fun nativeCall(request: Mono<BlockchainOuterClass.NativeCallRequest>): Flux<BlockchainOuterClass.NativeCallReplyItem> {
        return nativeCall.nativeCall(request)
    }

    override fun subscribeHead(request: Mono<Common.Chain>): Flux<BlockchainOuterClass.ChainHead> {
        return streamHead.add(request)
    }

    override fun subscribeTxStatus(request: Mono<BlockchainOuterClass.TxStatusRequest>): Flux<BlockchainOuterClass.TxStatus> {
        return trackTx.add(request)
    }

    override fun subscribeBalance(request: Mono<BlockchainOuterClass.BalanceRequest>): Flux<BlockchainOuterClass.AddressBalance> {
        return trackAddress.subscribe(request)
    }

    override fun getBalance(request: Mono<BlockchainOuterClass.BalanceRequest>): Flux<BlockchainOuterClass.AddressBalance> {
        return trackAddress.getBalance(request)
    }

    override fun describe(request: Mono<BlockchainOuterClass.DescribeRequest>): Mono<BlockchainOuterClass.DescribeResponse> {
        return describe.describe(request)
    }

    override fun subscribeStatus(request: Mono<BlockchainOuterClass.StatusRequest>): Flux<BlockchainOuterClass.ChainStatus> {
        return subscribeStatus.subscribeStatus(request)
    }
}