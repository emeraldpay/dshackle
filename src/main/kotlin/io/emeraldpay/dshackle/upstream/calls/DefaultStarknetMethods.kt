/**
 * Copyright (c) 2020 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.upstream.calls

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.quorum.AlwaysQuorum
import io.emeraldpay.dshackle.quorum.BroadcastQuorum
import io.emeraldpay.dshackle.quorum.CallQuorum
import io.emeraldpay.dshackle.quorum.MaximumValueQuorum
import io.emeraldpay.dshackle.quorum.NotNullQuorum
import io.emeraldpay.dshackle.upstream.ethereum.rpc.RpcException

/**
 * Default configuration for Ethereum based RPC. Defines optimal Quorum strategies for different methods, and provides
 * hardcoded results for base methods, such as `net_version`, `web3_clientVersion` and similar
 */
class DefaultStarknetMethods(
    private val chain: Chain,
) : CallMethods {

    private val nonce = setOf(
        "starknet_getNonce",
    )

    private val add = setOf(
        "starknet_addDeployAccountTransaction",
        "starknet_addDeclareTransaction",
        "starknet_addInvokeTransaction",
    )

    private val anyResponseMethods = listOf(
        "starknet_estimateFee",
        "starknet_estimateMessageFee",
        "starknet_specVersion",
    )

    private val nonNull = listOf(
        "starknet_getBlockWithTxHashes",
        "starknet_getBlockWithTxs",
        "starknet_getTransactionStatus",
        "starknet_getTransactionByHash",
        "starknet_getTransactionByBlockIdAndIndex",
        "starknet_getTransactionReceipt",
        "starknet_getBlockTransactionCount",
    )

    private val firstValueMethods = listOf(
        "starknet_call",
        "starknet_getEvents",
        "starknet_getStateUpdate",
        "starknet_getStorageAt",
        "starknet_getClass",
        "starknet_getClassHashAt",
        "starknet_getClassAt",
    )

    private val nonLagging = listOf(
        "starknet_blockNumber",
        "starknet_blockHashAndNumber",
    )

    private val hardcodedMethods = listOf(
        "starknet_chainId",
        "starknet_syncing",
    )

    private val allowedMethods: List<String> = anyResponseMethods + firstValueMethods + nonNull + nonce +
        add + nonLagging

    override fun createQuorumFor(method: String): CallQuorum {
        return when {
            nonce.contains(method) -> MaximumValueQuorum()
            add.contains(method) -> BroadcastQuorum()
            nonNull.contains(method) -> NotNullQuorum()

            else -> AlwaysQuorum()
        }
    }

    override fun isCallable(method: String): Boolean {
        return allowedMethods.contains(method)
    }

    override fun isHardcoded(method: String): Boolean {
        return hardcodedMethods.contains(method)
    }

    override fun executeHardcoded(method: String): ByteArray {
        val json = when (method) {
            "starknet_chainId" -> "\"${chain.chainId}\""
            "starknet_syncing" -> {
                "false"
            }
            else -> throw RpcException(-32601, "Method not found")
        }
        return json.toByteArray()
    }

    override fun getGroupMethods(groupName: String): Set<String> =
        when (groupName) {
            "default" -> getSupportedMethods()
            else -> emptyList()
        }.toSet()

    override fun getSupportedMethods(): Set<String> {
        return allowedMethods.plus(hardcodedMethods).toSortedSet()
    }
}
