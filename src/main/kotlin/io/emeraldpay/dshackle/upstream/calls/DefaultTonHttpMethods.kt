package io.emeraldpay.dshackle.upstream.calls

import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.quorum.AlwaysQuorum
import io.emeraldpay.dshackle.quorum.CallQuorum
import io.emeraldpay.dshackle.upstream.ethereum.rpc.RpcException

class DefaultTonHttpMethods(
    private val upstreamConnection: UpstreamsConfig.UpstreamConnection?,
) : CallMethods {

    // HTTP API section
    private val accountHttpMethods = setOf(
        getMethod("/getAddressInformation"),
        getMethod("/getExtendedAddressInformation"),
        getMethod("/getWalletInformation"),
        getMethod("/getAddressBalance"),
        getMethod("/getAddressState"),
        getMethod("/packAddress"),
        getMethod("/unpackAddress"),
        getMethod("/getTokenData"),
        getMethod("/detectAddress"),
    )

    private val blockHttpMethods = setOf(
        getMethod("/getMasterchainInfo"),
        getMethod("/getMasterchainBlockSignatures"),
        getMethod("/getShardBlockProof"),
        getMethod("/getConsensusBlock"),
        getMethod("/lookupBlock"),
        getMethod("/shards"),
        getMethod("/getBlockHeader"),
        getMethod("/getOutMsgQueueSizes"),
    )

    private val transactionHttpMethods = setOf(
        getMethod("/getTransactions"),
        getMethod("/getBlockTransactions"),
        getMethod("/getBlockTransactionsExt"),
        getMethod("/tryLocateTx"),
        getMethod("/tryLocateResultTx"),
        getMethod("/tryLocateSourceTx"),
    )

    private val getConfigHttpMethods = setOf(
        getMethod("/getConfigParam"),
        getMethod("/getConfigAll"),
    )

    private val runMethodHttpMethods = setOf(
        postMethod("/runGetMethod"),
    )

    private val sendHttpMethods = setOf(
        postMethod("/sendBoc"),
        postMethod("/sendBocReturnHash"),
        postMethod("/sendQuery"),
        postMethod("/estimateFee"),
    )

    private val jsonRpcHttpMethods = setOf(
        postMethod("/jsonRPC"),
    )

    // indexer v3 methods

    private val indexerAccountsMethods = setOf(
        getMethod("/api/v3/accountStates"),
        getMethod("/api/v3/addressBook"),
        getMethod("/api/v3/walletStates"),
    )

    private val indexerEventsMethods = setOf(
        getMethod("/api/v3/actions"),
        getMethod("/api/v3/events"),
    )

    private val indexerApiV2Methods = setOf(
        getMethod("/api/v3/addressInformation"),
        postMethod("/api/v3/estimateFee"),
        postMethod("/api/v3/message"),
        postMethod("/api/v3/runGetMethod"),
        getMethod("/api/v3/walletInformation"),
    )

    private val indexerBlockchainMethods = setOf(
        getMethod("/api/v3/adjacentTransactions"),
        getMethod("/api/v3/blocks"),
        getMethod("/api/v3/masterchainBlockShardState"),
        getMethod("/api/v3/masterchainBlockShards"),
        getMethod("/api/v3/masterchainInfo"),
        getMethod("/api/v3/messages"),
        getMethod("/api/v3/transactions"),
        getMethod("/api/v3/transactionsByMasterchainBlock"),
        getMethod("/api/v3/transactionsByMessage"),
    )

    private val indexerJettonsMethods = setOf(
        getMethod("/api/v3/jetton/burns"),
        getMethod("/api/v3/jetton/masters"),
        getMethod("/api/v3/jetton/transfers"),
        getMethod("/api/v3/jetton/wallets"),
    )

    private val indexerNftsMethods = setOf(
        getMethod("/api/v3/nft/collections"),
        getMethod("/api/v3/nft/items"),
        getMethod("/api/v3/nft/transfers"),
    )

    private val indexerStatsMethods = setOf(
        getMethod("/api/v3/topAccountsByBalance"),
    )

    private val indexerMethods = indexerAccountsMethods + indexerEventsMethods + indexerApiV2Methods +
        indexerBlockchainMethods + indexerJettonsMethods + indexerNftsMethods + indexerStatsMethods

    private val allowedHttpMethods: Set<String> = accountHttpMethods +
        blockHttpMethods +
        transactionHttpMethods +
        getConfigHttpMethods +
        runMethodHttpMethods +
        sendHttpMethods +
        jsonRpcHttpMethods +
        v3Methods()

    override fun createQuorumFor(method: String): CallQuorum {
        return AlwaysQuorum()
    }

    override fun isCallable(method: String): Boolean {
        return allowedHttpMethods.contains(method)
    }

    override fun getSupportedMethods(): Set<String> {
        return allowedHttpMethods.toSortedSet()
    }

    override fun isHardcoded(method: String): Boolean {
        return false
    }

    override fun executeHardcoded(method: String): ByteArray {
        throw RpcException(-32601, "Method not found")
    }

    override fun getGroupMethods(groupName: String): Set<String> {
        return when (groupName) {
            "default" -> allowedHttpMethods
            else -> emptyList()
        }.toSet()
    }

    private fun getMethod(method: String) = "GET#$method"

    private fun postMethod(method: String) = "POST#$method"

    private fun v3Methods(): Set<String> {
        return if (upstreamConnection is UpstreamsConfig.RpcConnection && upstreamConnection.getEndpointByTag("ton_v3") != null) {
            indexerMethods
        } else {
            emptySet()
        }
    }
}
