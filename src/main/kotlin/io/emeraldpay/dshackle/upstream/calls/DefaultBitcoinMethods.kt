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
package io.emeraldpay.dshackle.upstream.calls

import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.quorum.AlwaysQuorum
import io.emeraldpay.dshackle.quorum.BroadcastQuorum
import io.emeraldpay.dshackle.quorum.CallQuorum
import io.emeraldpay.dshackle.quorum.NonEmptyQuorum
import io.emeraldpay.dshackle.quorum.NotLaggingQuorum
import io.emeraldpay.etherjar.rpc.RpcException
import java.util.Collections

class DefaultBitcoinMethods : CallMethods {

    private val networkinfo = Global.objectMapper.writeValueAsBytes(
        mapOf(
            "version" to 210100,
            "subversion" to "/EmeraldDshackle:${Global.version}/"
        )
    )

    private val freshMethods = listOf(
        "getblock",
        "gettransaction", "gettxout",
        "getmemorypool"
    ).sorted()

    private val anyResponseMethods = listOf(
        "getblockhash", "getrawtransaction"
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

    private val allowedMethods =
        (freshMethods + anyResponseMethods + headVerifiedMethods + broadcastMethods).sorted()

    override fun createQuorumFor(method: String): CallQuorum {
        return when {
            Collections.binarySearch(hardcodedMethods, method) >= 0 -> AlwaysQuorum()
            Collections.binarySearch(anyResponseMethods, method) >= 0 -> NonEmptyQuorum()
            Collections.binarySearch(freshMethods, method) >= 0 -> NotLaggingQuorum(2)
            Collections.binarySearch(headVerifiedMethods, method) >= 0 -> NotLaggingQuorum(0)
            Collections.binarySearch(broadcastMethods, method) >= 0 -> BroadcastQuorum()
            else -> AlwaysQuorum()
        }
    }

    override fun isCallable(method: String): Boolean {
        return Collections.binarySearch(allowedMethods, method) >= 0
    }

    override fun getSupportedMethods(): Set<String> {
        return allowedMethods.plus(hardcodedMethods).toSortedSet()
    }

    override fun isHardcoded(method: String): Boolean {
        return Collections.binarySearch(hardcodedMethods, method) >= 0
    }

    override fun executeHardcoded(method: String): ByteArray {
        return when (method) {
            "getconnectioncount" -> "42".toByteArray()
            "getnetworkinfo" -> networkinfo
            else -> throw RpcException(-32601, "Method not found")
        }
    }

    override fun getGroupMethods(groupName: String): Set<String> = emptySet()
}
