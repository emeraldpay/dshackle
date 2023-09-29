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
package io.emeraldpay.dshackle.upstream.ethereum

import reactor.core.publisher.Flux

/**
 * A JSON-RPC Subscription client.
 * In general, it's a Websocket extension for JSON RPC that allows multiple responses to the same method.
 *
 * Example:
 *
 * <pre><code>
 * > {"id": 1, "method": "eth_subscribe", "params": ["newPendingTransactions"]}
 * > {"jsonrpc":"2.0","id":1,"result":"0xcff45d00e77e8e050d919daf284516c8"}
 * > {"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xcff45d00e77e8e050d919daf284516c8","result":"0xa61bab14fc9720ea8725622688c2f964666d7c2afdae38af7dad53f12f242d5c"}}
 * > {"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xcff45d00e77e8e050d919daf284516c8","result":"0x911548eb0f3bf353a54e03a3506c7c3e747470d6c201f03babbc07ff6e14cd6e"}}
 * > {"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xcff45d00e77e8e050d919daf284516c8","result":"0x67f22a3b441ea312306f97694ca8159f8d6faaccf0f5ce6442c84b13991f1d23"}}
 * </code></pre>
 *
 * */
interface WsSubscriptions {

    /**
     * Subscribe on remote
     */
    fun subscribe(method: String): SubscribeData

    fun connectionInfoFlux(): Flux<WsConnection.ConnectionInfo>

    data class SubscribeData(
        val data: Flux<ByteArray>,
        val connectionId: String,
    )
}
