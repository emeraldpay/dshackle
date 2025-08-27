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
package io.emeraldpay.dshackle.upstream.bitcoin.subscribe

import io.emeraldpay.api.Chain
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.ReactorBlockchainGrpc
import reactor.core.publisher.Flux

class BitcoinDshackleSubscriptionSource(
    blockchain: Chain,
    private val conn: ReactorBlockchainGrpc.ReactorBlockchainStub,
    topic: BitcoinZmqTopic,
) : BitcoinSubscriptionConnect<ByteArray>(topic) {
    private val request =
        BlockchainOuterClass.NativeSubscribeRequest
            .newBuilder()
            .setChainValue(blockchain.id)
            .setMethod(topic.id)
            .build()

    override fun createConnection(): Flux<ByteArray> =
        conn
            .nativeSubscribe(request)
            .map(::readResponse)

    private fun readResponse(resp: BlockchainOuterClass.NativeSubscribeReplyItem): ByteArray {
        // comes as a string, so cut off the quotes
        return resp.payload.toByteArray()
    }
}
