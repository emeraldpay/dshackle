package io.emeraldpay.dshackle.upstream.calls

import io.emeraldpay.dshackle.quorum.AlwaysQuorum
import io.emeraldpay.dshackle.quorum.CallQuorum
import io.emeraldpay.dshackle.upstream.ethereum.rpc.RpcException

class DefaultTonHttpMethods : CallMethods {

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

    private val allowedHttpMethods: Set<String> = accountHttpMethods +
        blockHttpMethods +
        transactionHttpMethods +
        getConfigHttpMethods +
        runMethodHttpMethods +
        sendHttpMethods +
        jsonRpcHttpMethods

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
}
