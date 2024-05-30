package io.emeraldpay.dshackle.upstream.calls

import io.emeraldpay.dshackle.quorum.AlwaysQuorum
import io.emeraldpay.dshackle.quorum.BroadcastQuorum
import io.emeraldpay.dshackle.quorum.CallQuorum
import io.emeraldpay.dshackle.upstream.ethereum.rpc.RpcException

class DefaultCosmosMethods : CallMethods {
    private val all = setOf(
        "health",
        "status",
        "net_info",
        "blockchain",
        "block",
        "block_by_hash",
        "block_results",
        "commit",
        "validators",
        "genesis",
        "genesis_chunked",
        // "dump_consensus_state", // not safe
        //  "consensus_state",      // not safe
        "consensus_params",
        "unconfirmed_txs",
        "num_unconfirmed_txs",
        "tx_search",
        "block_search",
        "tx",
        "check_tx",
        "abci_info",
        "abci_query",
    )

    private val add = setOf(
        "broadcast_evidence",
        "broadcast_tx_sync",
        "broadcast_tx_async",
        "broadcast_tx_commit",
    )

    private val allowedMethods: Set<String> = all + add

    override fun createQuorumFor(method: String): CallQuorum {
        if (add.contains(method)) {
            return BroadcastQuorum()
        }
        return AlwaysQuorum()
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
