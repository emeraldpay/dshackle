package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.quorum.AlwaysQuorum
import io.emeraldpay.dshackle.quorum.BroadcastQuorum
import io.emeraldpay.dshackle.quorum.CallQuorum
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.ethereum.rpc.RpcException

class DefaultSolanaMethods : CallMethods {
    companion object {
        val subs = setOf(
            "accountSubscribe" to "accountUnsubscribe",
            "blockSubscribe" to "blockUnsubscribe",
            "logsSubscribe" to "logsUnsubscribe",
            "programSubscribe" to "programUnsubscribe",
            "signatureSubscribe" to "signatureUnsubscribe",
            "slotSubscribe" to "slotUnsubscribe",
        )
    }

    private val all = setOf(
        "getAccountInfo",
        "getBalance",
        "getBlock",
        "getBlockHeight",
        "getBlockProduction",
        "getBlockCommitment",
        "getBlocks",
        "getBlocksWithLimit",
        "getBlockTime",
        "getClusterNodes",
        "getEpochInfo",
        "getEpochSchedule",
        "getFeeForMessage",
        "getFirstAvailableBlock",
        "getGenesisHash",
        "getHealth",
        "getHighestSnapshotSlot",
        "getInflationGovernor",
        "getInflationRate",
        "getInflationReward",
        "getLargestAccounts",
        "getLatestBlockhash",
        "getLeaderSchedule",
        "getMaxRetransmitSlot",
        "getMaxShredInsertSlot",
        "getMinimumBalanceForRentExemption",
        "getMultipleAccounts",
        "getProgramAccounts",
        "getRecentPerformanceSamples",
        "getRecentPrioritizationFees",
        "getSignaturesForAddress",
        "getSignatureStatuses",
        "getSlot",
        "getSlotLeader",
        "getSlotLeaders",
        "getStakeActivation",
        "getStakeMinimumDelegation",
        "getSupply",
        "getTokenAccountBalance",
        "getTokenAccountsByDelegate",
        "getTokenAccountsByOwner",
        "getTokenLargestAccounts",
        "getTokenSupply",
        "getTransaction",
        "getTransactionCount",
        "getVersion",
        "getVoteAccounts",
        "isBlockhashValid",
        "minimumLedgerSlot",
        "requestAirdrop",
        "simulateTransaction",
        "getRecentBlockHash",
        "getFees",
        "getIdentity",
        "getConfirmedSignaturesForAddress2",
    )

    private val add = setOf(
        "sendTransaction",
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
