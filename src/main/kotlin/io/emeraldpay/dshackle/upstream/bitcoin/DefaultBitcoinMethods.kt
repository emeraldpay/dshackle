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
package io.emeraldpay.dshackle.upstream.bitcoin

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.quorum.*
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.infinitape.etherjar.rpc.JacksonRpcConverter
import io.infinitape.etherjar.rpc.RpcException
import java.util.*

class DefaultBitcoinMethods(
        private val objectMapper: ObjectMapper
) : CallMethods {

    //TODO maybe Ethereum RPC parser should not be really used for Bitcoin
    private val jacksonRpcConverter = JacksonRpcConverter(objectMapper)

    private val anyResponseMethods = listOf(
            "getblock",
            "gettransaction", "getrawtransaction", "gettxout",
            "getmemorypool"
    ).sorted()

    private val headVerifiedMethods = listOf(
            "getbestblockhash", "getblocknumber", "getblockcount",
            "listunspent", "getreceivedbyaddress"
    ).sorted()

    private val hardcodedMethods = listOf(
            "getconnectioncount", "getnetworkinfo"
    ).sorted()

    private val broadcastMethods = listOf(
            "sendrawtransaction"
    ).sorted()

    private val allowedMethods = (anyResponseMethods + hardcodedMethods + headVerifiedMethods).sorted()

    override fun getQuorumFor(method: String): CallQuorum {
        return when {
            Collections.binarySearch(hardcodedMethods, method) >= 0 -> AlwaysQuorum()
            Collections.binarySearch(anyResponseMethods, method) >= 0 -> NotLaggingQuorum(2)
            Collections.binarySearch(headVerifiedMethods, method) >= 0 -> NotLaggingQuorum(0)
            Collections.binarySearch(broadcastMethods, method) >= 0 -> BroadcastQuorum(jacksonRpcConverter)
            else -> AlwaysQuorum()
        }
    }

    override fun isAllowed(method: String): Boolean {
        return Collections.binarySearch(allowedMethods, method) >= 0
    }

    override fun getSupportedMethods(): Set<String> {
        return allowedMethods.toSortedSet()
    }

    override fun isHardcoded(method: String): Boolean {
        return Collections.binarySearch(hardcodedMethods, method) >= 0;
    }

    override fun executeHardcoded(method: String): Any {
        return when (method) {
            "getconnectioncount" -> 42
            "getnetworkinfo" -> mapOf(
                    "version" to 700000,
                    "subversion" to "/EmeraldDshackle:v0.7/"
            )
            else -> throw RpcException(-32601, "Method not found")
        }
    }

}