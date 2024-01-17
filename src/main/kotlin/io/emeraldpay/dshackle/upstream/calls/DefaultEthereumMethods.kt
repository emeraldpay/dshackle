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
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.quorum.AlwaysQuorum
import io.emeraldpay.dshackle.quorum.BroadcastQuorum
import io.emeraldpay.dshackle.quorum.CallQuorum
import io.emeraldpay.dshackle.quorum.MaximumValueQuorum
import io.emeraldpay.dshackle.quorum.NotNullQuorum
import io.emeraldpay.etherjar.rpc.RpcException

/**
 * Default configuration for Ethereum based RPC. Defines optimal Quorum strategies for different methods, and provides
 * hardcoded results for base methods, such as `net_version`, `web3_clientVersion` and similar
 */
class DefaultEthereumMethods(
    private val chain: Chain,
    private val hasLogsOracle: Boolean = false,
) : CallMethods {

    private val version = "\"EmeraldDshackle/${Global.version}\""

    companion object {
        val withFilterIdMethods = listOf(
            "eth_getFilterChanges",
            "eth_getFilterLogs",
            "eth_uninstallFilter",
        )

        val newFilterMethods = listOf(
            "eth_newFilter",
            "eth_newBlockFilter",
            "eth_newPendingTransactionFilter",
        )

        val traceMethods = listOf(
            "trace_call",
            "trace_callMany",
            "trace_rawTransaction",
            "trace_replayBlockTransactions",
            "trace_replayTransaction",
            "trace_block",
            "trace_filter",
            "trace_get",
            "trace_transaction",
        )

        val debugMethods = listOf(
            "debug_storageRangeAt",
            "debug_traceBlock",
            "debug_traceBlockByHash",
            "debug_traceBlockByNumber",
            "debug_traceCall",
            "debug_traceCallMany",
            "debug_traceTransaction",
        )
    }

    private val anyResponseMethods = listOf(
        "eth_gasPrice",
        "eth_estimateGas",
    )

    private val possibleNotIndexedMethods = listOf(
        "eth_getTransactionByHash",
        "eth_getTransactionReceipt",
        "eth_getBlockTransactionCountByHash",
        "eth_getBlockByHash",
        "eth_getBlockByNumber",
        "eth_getTransactionByBlockHashAndIndex",
        "eth_getTransactionByBlockNumberAndIndex",
        "eth_getUncleByBlockHashAndIndex",
        "eth_getUncleCountByBlockHash",
        "eth_getBlockReceipts",
    )

    private val firstValueMethods = listOf(
        "eth_call",
        "eth_getStorageAt",
        "eth_getCode",
        "eth_getLogs",
        "eth_maxPriorityFeePerGas",
        "eth_getProof",
    )

    private val specialMethods = listOf(
        "eth_getTransactionCount",
        "eth_blockNumber",
        "eth_getBalance",
        "eth_sendRawTransaction",
    )

    private val headVerifiedMethods = listOf(
        "eth_getBlockTransactionCountByNumber",
        "eth_getUncleCountByBlockNumber",
        "eth_getUncleByBlockNumberAndIndex",
        "eth_feeHistory",
    )

    private val filterMethods = withFilterIdMethods + newFilterMethods

    private val hardcodedMethods = listOf(
        "net_version",
        "net_peerCount",
        "net_listening",
        "web3_clientVersion",
        "eth_protocolVersion",
        "eth_syncing",
        "eth_coinbase",
        "eth_mining",
        "eth_hashrate",
        "eth_accounts",
        "eth_chainId",
    )

    private val allowedMethods: List<String>

    init {
        allowedMethods = anyResponseMethods +
            firstValueMethods +
            possibleNotIndexedMethods +
            specialMethods +
            headVerifiedMethods -
            chainUnsupportedMethods(chain) +
            getDrpcVendorMethods(chain) +
            getChainSpecificMethods(chain)
    }

    override fun createQuorumFor(method: String): CallQuorum {
        return when {
            possibleNotIndexedMethods.contains(method) -> NotNullQuorum()
            specialMethods.contains(method) -> {
                when (method) {
                    "eth_getTransactionCount" -> MaximumValueQuorum()
                    "eth_sendRawTransaction" -> BroadcastQuorum()
                    else -> AlwaysQuorum()
                }
            }

            else -> AlwaysQuorum()
        }
    }

    private fun getDrpcVendorMethods(chain: Chain): List<String> {
        val supported = mutableListOf<String>()

        // Currently tested on eth mainnet only, should potentially work for all compatible ones.
        if (chain == Chain.ETHEREUM__MAINNET && hasLogsOracle) { supported.add("drpc_getLogsEstimate") }

        return supported
    }

    private fun getChainSpecificMethods(chain: Chain): List<String> {
        return when (chain) {
            Chain.OPTIMISM__MAINNET, Chain.OPTIMISM__GOERLI, Chain.MANTLE__MAINNET, Chain.MANTLE__TESTNET ->
                listOf(
                    "rollup_gasPrices",
                )
            Chain.POLYGON__MAINNET, Chain.POLYGON__MUMBAI -> listOf(
                "bor_getAuthor",
                "bor_getCurrentValidators",
                "bor_getCurrentProposer",
                "bor_getRootHash",
                "bor_getSignersAtHash",
                "eth_getRootHash",
            )

            Chain.POLYGON_ZKEVM__MAINNET, Chain.POLYGON_ZKEVM__TESTNET -> listOf(
                "zkevm_consolidatedBlockNumber",
                "zkevm_isBlockConsolidated",
                "zkevm_isBlockVirtualized",
                "zkevm_batchNumberByBlockNumber",
                "zkevm_batchNumber",
                "zkevm_virtualBatchNumber",
                "zkevm_verifiedBatchNumber",
                "zkevm_getBatchByNumber",
                "zkevm_getBroadcastURI",
            )

            Chain.ZKSYNC__MAINNET, Chain.ZKSYNC__TESTNET -> listOf(
                "zks_estimateFee",
                "zks_estimateGasL1ToL2",
                "zks_getAllAccountBalances",
                "zks_getBlockDetails",
                "zks_getBridgeContracts",
                "zks_getBytecodeByHash",
                "zks_getConfirmedTokens",
                "zks_getL1BatchBlockRange",
                "zks_getL1BatchDetails",
                "zks_getL2ToL1LogProof",
                "zks_getL2ToL1MsgProof",
                "zks_getMainContract",
                "zks_getRawBlockTransactions",
                "zks_getTestnetPaymaster",
                "zks_getTokenPrice",
                "zks_getTransactionDetails",
                "zks_L1BatchNumber",
                "zks_L1ChainId",
            )

            else -> emptyList()
        }
    }

    private fun chainUnsupportedMethods(chain: Chain): Set<String> {
        return when (chain) {
            Chain.OPTIMISM__MAINNET -> setOf("eth_getAccounts")
            Chain.ZKSYNC__MAINNET, Chain.ZKSYNC__TESTNET, Chain.POLYGON_ZKEVM__TESTNET, Chain.POLYGON_ZKEVM__MAINNET ->
                setOf("eth_maxPriorityFeePerGas")
            else -> emptySet()
        }
    }

    override fun isCallable(method: String): Boolean {
        return allowedMethods.contains(method)
    }

    override fun isHardcoded(method: String): Boolean {
        return hardcodedMethods.contains(method)
    }

    override fun executeHardcoded(method: String): ByteArray {
        // note that the value is in json representation, i.e. if it's a string it should be with quotes,
        // that's why "\"0x0\"", "\"1\"", etc. But just "true" for a boolean, or "[]" for array.
        val json = when (method) {
            "net_version" -> "\"${chain.netVersion}\""
            "eth_chainId" -> "\"${chain.chainId}\""

            "net_peerCount" -> {
                "\"0x2a\""
            }

            "net_listening" -> {
                "true"
            }

            "web3_clientVersion" -> {
                version
            }

            "eth_protocolVersion" -> {
                "\"0x3f\""
            }

            "eth_syncing" -> {
                "false"
            }

            "eth_coinbase" -> {
                "\"0x0000000000000000000000000000000000000000\""
            }

            "eth_mining" -> {
                "false"
            }

            "eth_hashrate" -> {
                "\"0x0\""
            }

            "eth_accounts" -> {
                "[]"
            }

            else -> throw RpcException(-32601, "Method not found")
        }
        return json.toByteArray()
    }

    override fun getGroupMethods(groupName: String): Set<String> =
        when (groupName) {
            "filter" -> filterMethods
            "trace" -> traceMethods
            "debug" -> debugMethods
            "default" -> getSupportedMethods()
            else -> emptyList()
        }.toSet()

    override fun getSupportedMethods(): Set<String> {
        return allowedMethods.plus(hardcodedMethods).toSortedSet()
    }

    data class HardcodedData private constructor(
        val netVersion: String,
        val chainId: String,
    ) {
        companion object {
            fun createHardcodedData(netVersion: String, chainId: String): HardcodedData =
                HardcodedData(netVersion.lowercase(), chainId.lowercase())
        }
    }
}
