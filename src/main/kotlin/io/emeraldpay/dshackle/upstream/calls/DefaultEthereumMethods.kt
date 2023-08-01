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
import io.emeraldpay.dshackle.quorum.NotLaggingQuorum
import io.emeraldpay.dshackle.quorum.NotNullQuorum
import io.emeraldpay.etherjar.rpc.RpcException

/**
 * Default configuration for Ethereum based RPC. Defines optimal Quorum strategies for different methods, and provides
 * hardcoded results for base methods, such as `net_version`, `web3_clientVersion` and similar
 */
class DefaultEthereumMethods(
    private val chain: Chain
) : CallMethods {

    private val version = "\"EmeraldDshackle/${Global.version}\""

    companion object {
        val withFilterIdMethods = listOf(
            "eth_getFilterChanges",
            "eth_getFilterLogs",
            "eth_uninstallFilter"
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
            "debug_traceTransaction"
        )

        val CHAIN_DATA = mapOf(
            Chain.ETHEREUM__MAINNET to HardcodedData("\"1\"", "\"0x1\""),
            Chain.ETHEREUM__RINKEBY to HardcodedData("\"4\"", "\"0x4\""),
            Chain.ETHEREUM__ROPSTEN to HardcodedData("\"3\"", "\"0x3\""),
            Chain.ETHEREUM__KOVAN to HardcodedData("\"42\"", "\"0x2a\""),
            Chain.ETHEREUM__GOERLI to HardcodedData("\"5\"", "\"0x5\""),
            Chain.ETHEREUM__SEPOLIA to HardcodedData("\"11155111\"", "\"0xaa36a7\""),

            Chain.ETHEREUM_CLASSIC__MAINNET to HardcodedData("\"1\"", "\"0x3d\""),

            Chain.POLYGON_POS__MAINNET to HardcodedData("\"137\"", "\"0x89\""),
            Chain.POLYGON_POS__MUMBAI to HardcodedData("\"80001\"", "\"0x13881\""),

            Chain.ARBITRUM__MAINNET to HardcodedData("\"42161\"", "\"0xa4b1\""),
            Chain.ARBITRUM__GOERLI to HardcodedData("\"421613\"", "\"0x66eed\""),

            Chain.OPTIMISM__MAINNET to HardcodedData("\"10\"", "\"0xa\""),
            Chain.OPTIMISM__GOERLI to HardcodedData("\"420\"", "\"0x1A4\""),

            Chain.ARBITRUM_NOVA__MAINNET to HardcodedData("\"42170\"", "\"0xa4ba\""),

            Chain.POLYGON_ZKEVM__MAINNET to HardcodedData("\"1101\"", "\"0x44d\""),
            Chain.POLYGON_ZKEVM__TESTNET to HardcodedData("\"1442\"", "\"0x5a2\""),

            Chain.ZKSYNC__MAINNET to HardcodedData("\"324\"", "\"0x144\""),
            Chain.ZKSYNC__TESTNET to HardcodedData("\"280\"", "\"0x118\""),

            Chain.BSC__MAINNET to HardcodedData("\"56\"", "\"0x38\""),

            Chain.BASE__MAINNET to HardcodedData("\"8453\"", "\"0x2105\""),
            Chain.BASE__GOERLI to HardcodedData("\"84531\"", "\"0x14a33\""),

            Chain.LINEA__MAINNET to HardcodedData("\"59144\"", "\"0xe708\""),
            Chain.LINEA__GOERLI to HardcodedData("\"59140\"", "\"0xe704\""),

            Chain.FANTOM__MAINNET to HardcodedData("\"250\"", "\"0xfa\""),
            Chain.FANTOM__TESTNET to HardcodedData("\"4002\"", "\"0xfa2\""),

            Chain.GNOSIS__MAINNET to HardcodedData("\"100\"", "\"0x64\""),
            Chain.GNOSIS__CHIADO to HardcodedData("\"10200\"", "\"0x27d8\""),

            Chain.AVALANCHE__MAINNET to HardcodedData("\"43114\"", "\"0xa86a\""),
            Chain.AVALANCHE__FUJI to HardcodedData("\"43113\"", "\"0xa869\""),
        )

        fun getChainByData(data: HardcodedData) = CHAIN_DATA.entries.find { it.value == data }?.key
    }

    private val anyResponseMethods = listOf(
        "eth_gasPrice",
        "eth_estimateGas"
    )

    private val possibleNotIndexedMethods = listOf(
        "eth_getTransactionByHash",
        "eth_getTransactionReceipt"
    )

    private val firstValueMethods = listOf(
        "eth_call",
        "eth_getBlockTransactionCountByHash",
        "eth_getUncleCountByBlockHash",
        "eth_getBlockByHash",
        "eth_getBlockByNumber",
        "eth_getTransactionByBlockHashAndIndex",
        "eth_getTransactionByBlockNumberAndIndex",
        "eth_getStorageAt",
        "eth_getCode",
        "eth_getUncleByBlockHashAndIndex",
        "eth_getLogs",
        "eth_maxPriorityFeePerGas"
    )

    private val specialMethods = listOf(
        "eth_getTransactionCount",
        "eth_blockNumber",
        "eth_getBalance",
        "eth_sendRawTransaction"
    )

    private val headVerifiedMethods = listOf(
        "eth_getBlockTransactionCountByNumber",
        "eth_getUncleCountByBlockNumber",
        "eth_getUncleByBlockNumberAndIndex",
        "eth_feeHistory"
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
        "eth_chainId"
    )

    private val allowedMethods: List<String>

    init {
        allowedMethods = anyResponseMethods +
            firstValueMethods +
            possibleNotIndexedMethods +
            specialMethods +
            headVerifiedMethods -
            chainUnsupportedMethods(chain) +
            getChainSpecificMethods(chain)
    }

    override fun createQuorumFor(method: String): CallQuorum {
        return when {
            newFilterMethods.contains(method) -> NotLaggingQuorum(4)
            withFilterIdMethods.contains(method) -> AlwaysQuorum()
            hardcodedMethods.contains(method) -> AlwaysQuorum()
            firstValueMethods.contains(method) -> AlwaysQuorum()
            anyResponseMethods.contains(method) -> NotLaggingQuorum(4)
            headVerifiedMethods.contains(method) -> NotLaggingQuorum(1)
            possibleNotIndexedMethods.contains(method) -> NotNullQuorum()
            specialMethods.contains(method) -> {
                when (method) {
                    "eth_getTransactionCount" -> AlwaysQuorum()
                    "eth_getBalance" -> AlwaysQuorum()
                    "eth_sendRawTransaction" -> BroadcastQuorum()
                    "eth_blockNumber" -> NotLaggingQuorum(0)
                    else -> AlwaysQuorum()
                }
            }

            getChainSpecificMethods(chain).contains(method) -> {
                when (method) {
                    "bor_getAuthor" -> NotLaggingQuorum(4)
                    "bor_getCurrentValidators" -> NotLaggingQuorum(0)
                    "bor_getCurrentProposer" -> NotLaggingQuorum(0)
                    "bor_getRootHash" -> NotLaggingQuorum(4)
                    "eth_getRootHash" -> NotLaggingQuorum(4)
                    else -> AlwaysQuorum()
                }
            }

            else -> AlwaysQuorum()
        }
    }

    private fun getChainSpecificMethods(chain: Chain): List<String> {
        return when (chain) {
            Chain.OPTIMISM__MAINNET, Chain.OPTIMISM__GOERLI -> listOf(
                "rollup_gasPrices"
            )

            Chain.POLYGON_POS__MAINNET, Chain.POLYGON_POS__MUMBAI -> listOf(
                "bor_getAuthor",
                "bor_getCurrentValidators",
                "bor_getCurrentProposer",
                "bor_getRootHash",
                "bor_getSignersAtHash",
                "eth_getRootHash"
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
                "zkevm_getBroadcastURI"
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
                "zks_L1ChainId"
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

    data class HardcodedData(val netVersion: String, val chainId: String)

    override fun executeHardcoded(method: String): ByteArray {
        // note that the value is in json representation, i.e. if it's a string it should be with quotes,
        // that's why "\"0x0\"", "\"1\"", etc. But just "true" for a boolean, or "[]" for array.
        val json = when (method) {
            "net_version" -> CHAIN_DATA.get(chain)?.netVersion ?: throw RpcException(-32602, "Invalid chain")
            "eth_chainId" -> CHAIN_DATA.get(chain)?.chainId ?: throw RpcException(-32602, "Invalid chain")

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
            else -> emptyList()
        }.toSet()

    override fun getSupportedMethods(): Set<String> {
        return allowedMethods.plus(hardcodedMethods).toSortedSet()
    }
}
