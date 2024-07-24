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
import io.emeraldpay.dshackle.upstream.ethereum.rpc.RpcException

/**
 * Default configuration for Ethereum based RPC. Defines optimal Quorum strategies for different methods, and provides
 * hardcoded results for base methods, such as `net_version`, `web3_clientVersion` and similar
 */
class DefaultPolkadotMethods(
    val chain: Chain,
) : CallMethods {

    companion object {
        val subs = setOf(
            "subscribe_newHead" to "unsubscribe_newHead",
            "chain_subscribeNewHead" to "chain_unsubscribeNewHead",
            "chain_subscribeAllHeads" to "chain_unsubscribeAllHeads",
            "chain_subscribeFinalizedHeads" to "chain_unsubscribeFinalizedHeads",
            "chain_subscribeFinalisedHeads" to "chain_unsubscribeFinalisedHeads",
            "chain_subscribeNewHeads" to "chain_unsubscribeNewHeads",
            "chain_subscribeRuntimeVersion" to "chain_unsubscribeRuntimeVersion",
            "author_submitAndWatchExtrinsic" to "author_unwatchExtrinsic",
            "grandpa_subscribeJustifications" to "grandpa_unsubscribeJustifications",
            "state_subscribeRuntimeVersion" to "state_unsubscribeRuntimeVersion",
            "state_subscribeStorage" to "state_unsubscribeStorage",
            "transaction_unstable_submitAndWatch" to "transaction_unstable_unwatch",
        )
    }

    private val all = setOf(
        "account_nextIndex",
        "author_hasKey",
        "author_hasSessionKeys",
        "author_insertKey",
        "author_pendingExtrinsics",
        "author_removeExtrinsic",
        "author_rotateKeys",
        "babe_epochAuthorship",
        "chainHead_unstable_body",
        "chainHead_unstable_call",
        "chainHead_unstable_follow",
        "chainHead_unstable_genesisHash",
        "chainHead_unstable_header",
        "chainHead_unstable_stopBody",
        "chainHead_unstable_stopCall",
        "chainHead_unstable_stopStorage",
        "chainHead_unstable_storage",
        "chainHead_unstable_unfollow",
        "chainHead_unstable_unpin",
        "chainSpec_unstable_chainName",
        "chainSpec_unstable_genesisHash",
        "chainSpec_unstable_properties",
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
        "dev_getBlockStats",
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
        "offchain_localStorageGet",
        "offchain_localStorageSet",
        "payment_queryFeeDetails",
        "payment_queryInfo",
        "runtime_wasmBlobVersion",
        "stakingRewards_inflationInfo",
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
        "sync_state_genSyncSpec",
        "system_accountNextIndex",
        "system_addLogFilter",
        "system_addReservedPeer",
        "system_chain",
        "system_chainType",
        "system_dryRun",
        "system_dryRunAt",
        "system_health",
        "system_localListenAddresses",
        "system_localPeerId",
        "system_name",
        "system_nodeRoles",
        "system_peers",
        "system_properties",
        "system_removeReservedPeer",
        "system_reservedPeers",
        "system_resetLogFilter",
        "system_syncState",
        "system_unstable_networkState",
        "system_version",
    )

    private val availMethods = setOf(
        "chainSpec_v1_chainName",
        "chainSpec_v1_genesisHash",
        "chainSpec_v1_properties",
        "kate_blockLength",
        "kate_queryDataProof",
        "kate_queryProof",
        "kate_queryRows",
        "mmr_generateProof",
        "mmr_root",
        "mmr_verifyProof",
        "mmr_verifyProofStateless",
    )

    private val add = setOf(
        "author_submitExtrinsic",
    )

    private val allowedMethods: Set<String> = all + add + specificMethods()

    private fun specificMethods(): Set<String> {
        return if (chain == Chain.AVAIL__MAINNET || chain == Chain.AVAIL__TESTNET) {
            availMethods
        } else {
            emptySet()
        }
    }

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
