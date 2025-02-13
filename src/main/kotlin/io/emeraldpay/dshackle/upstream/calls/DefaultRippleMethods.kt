package io.emeraldpay.dshackle.upstream.calls

import io.emeraldpay.dshackle.quorum.AlwaysQuorum
import io.emeraldpay.dshackle.quorum.BroadcastQuorum
import io.emeraldpay.dshackle.quorum.CallQuorum
import io.emeraldpay.dshackle.upstream.ethereum.rpc.RpcException

class DefaultRippleMethods : CallMethods {

    private val all = setOf(
        "account_channels",
        "account_currencies",
        "account_info",
        "account_lines",
        "account_nfts",
        "account_objects",
        "account_offers",
        "account_tx",
        "gateway_balances",
        "noripple_check",
        "ledger",
        "ledger_closed",
        "ledger_current",
        "ledger_data",
        "ledger_entry",
        "transaction_entry",
        "tx",
        "tx_history",
        "book_offers",
        "deposit_authorized",
        "nft_buy_offers",
        "nft_sell_offers",
        "path_find",
        "ripple_path_find",
        "channel_authorize",
        "channel_verify",
        "subscribe",
        "unsubscribe",
        "fee",
        "manifest",
        "server_info",
        "server_state",
        "ledger_index",
        "nft_history",
        "nft_info",
        "nfts_by_issuer",
        "ping",
        "random",
//        "amm_info",
//        "book_changes",
//        "get_aggregate_price",
//        "server_definitions",
//        "version",
//        "server_info",
//        "ledger",
//        "mpt_holders",
//        "version",
    )

    private val add = setOf(
        "submit",
        "submit_multisigned",
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
