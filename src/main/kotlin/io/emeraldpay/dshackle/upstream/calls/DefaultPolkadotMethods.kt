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

import io.emeraldpay.dshackle.quorum.AlwaysQuorum
import io.emeraldpay.dshackle.quorum.BroadcastQuorum
import io.emeraldpay.dshackle.quorum.CallQuorum
import io.emeraldpay.etherjar.rpc.RpcException

/**
 * Default configuration for Ethereum based RPC. Defines optimal Quorum strategies for different methods, and provides
 * hardcoded results for base methods, such as `net_version`, `web3_clientVersion` and similar
 */
class DefaultPolkadotMethods : CallMethods {

    companion object {
        val subs = setOf(
            Pair("subscribe_newHead", "unsubscribe_newHead"),
            Pair("chain_subscribeAllHeads", "chain_unsubscribeAllHeads"),
            Pair("chain_subscribeFinalizedHeads", "chain_unsubscribeFinalizedHeads"),
            Pair("chain_subscribeNewHeads", "chain_unsubscribeNewHeads"),
            Pair("chain_subscribeRuntimeVersion", "chain_unsubscribeNewHeads"),
            Pair("chain_subscribeRuntimeVersion", "chain_unsubscribeRuntimeVersion"),
        )
    }

    private val all = setOf(
        "author_pendingExtrinsics",
        "author_removeExtrinsic",
        "chain_getBlock",
        "chain_getBlockHash",
        "chain_getFinalisedHead",
        "chain_getFinalizedHead",
        "chain_getHead",
        "chain_getHeader",
        "chain_getRuntimeVersion",
        "childstate_getKeys",
        "childstate_getKeysPaged",
        "childstate_getKeysPagedAt",
        "childstate_getStorage",
        "childstate_getStorageEntries",
        "childstate_getStorageHash",
        "childstate_getStorageSize",
        "gear_calculateHandleGas",
        "gear_calculateInitCreateGas",
        "gear_calculateInitUploadGas",
        "gear_calculateReplyGas",
        "gear_readMetahash",
        "gear_readState",
        "gear_readStateBatch",
        "gear_readStateUsingWasm",
        "gear_readStateUsingWasmBatch",
        "grandpa_proveFinality",
        "grandpa_roundState",
        "payment_queryFeeDetails",
        "payment_queryInfo",
        "state_call",
        "state_callAt",
        "state_getChildReadProof",
        "state_getKeys",
        "state_getKeysPaged",
        "state_getKeysPagedAt",
        "state_getMetadata",
        "state_getPairs",
        "state_getReadProof",
        "state_getRuntimeVersion",
        "state_getStorage",
        "state_getStorageAt",
        "state_getStorageHash",
        "state_getStorageHashAt",
        "state_getStorageSize",
        "state_getStorageSizeAt",
        "state_queryStorage",
        "state_queryStorageAt",
        "state_traceBlock",
        "state_trieMigrationStatus",
        "system_chain",
    )

    private val add = setOf(
        "author_submitExtrinsic",
    )

    private val allowedMethods: Set<String> = all + add

    override fun createQuorumFor(method: String): CallQuorum {
        return when {
            add.contains(method) -> BroadcastQuorum()
            all.contains(method) -> AlwaysQuorum()
            else -> AlwaysQuorum()
        }
    }

    override fun isCallable(method: String): Boolean {
        return allowedMethods.contains(method)
    }

    override fun isHardcoded(method: String): Boolean {
        return false
    }

    override fun executeHardcoded(method: String): ByteArray {
        throw RpcException(-32601, "Method not found")
    }

    override fun getGroupMethods(groupName: String): Set<String> =
        when (groupName) {
            "default" -> getSupportedMethods()
            else -> emptyList()
        }.toSet()

    override fun getSupportedMethods(): Set<String> {
        return allowedMethods.toSortedSet()
    }
}
