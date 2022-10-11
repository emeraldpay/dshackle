/**
 * Copyright (c) 2022 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.upstream.ethereum.subscribe

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.BlockchainOuterClass.NativeSubscribeReplyItem
import io.emeraldpay.api.proto.BlockchainOuterClass.NativeSubscribeRequest
import io.emeraldpay.api.proto.ReactorBlockchainGrpc
import io.emeraldpay.dshackle.upstream.ethereum.EthereumSubscriptionApi
import io.emeraldpay.etherjar.domain.TransactionId
import io.emeraldpay.grpc.Chain
import reactor.core.publisher.Flux

class DshacklePendingTxesSource(
    private val blockchain: Chain,
    private val conn: ReactorBlockchainGrpc.ReactorBlockchainStub,
) : PendingTxesSource, DefaultPendingTxesSource() {

    private val request = NativeSubscribeRequest.newBuilder()
        .setChainValue(blockchain.id)
        .setMethod(EthereumSubscriptionApi.METHOD_PENDING_TXES)
        .build()

    var available = false

    override fun createConnection(): Flux<TransactionId> {
        if (!available) {
            return Flux.empty()
        }
        return conn
            .nativeSubscribe(request)
            .map(::readResponse)
            .map(TransactionId::from)
    }

    fun readResponse(resp: NativeSubscribeReplyItem): String {
        // comes as a string, so cut off the quotes
        return resp.payload.substring(1, resp.payload.size() - 1).toStringUtf8()
    }

    fun update(conf: BlockchainOuterClass.DescribeChain) {
        available = conf.supportedMethodsList.any {
            it == EthereumSubscriptionApi.METHOD_PENDING_TXES
        }
    }
}
