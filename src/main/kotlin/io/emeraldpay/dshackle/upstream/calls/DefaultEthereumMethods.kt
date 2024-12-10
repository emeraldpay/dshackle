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
import io.emeraldpay.dshackle.upstream.ethereum.rpc.RpcException

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

        var arbitrumTraceMethods = listOf(
            "arbtrace_call",
            "arbtrace_callMany",
            "arbtrace_replayBlockTransactions",
            "arbtrace_replayTransaction",
            "arbtrace_block",
            "arbtrace_filter",
            "arbtrace_get",
            "arbtrace_transaction",
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
    )

    private val harmonyPossibleNotIndexedMethods = listOf(
        "hmy_getStakingTransactionByBlockHashAndIndex",
        "hmy_getStakingTransactionByHash",
        "hmy_getCXReceiptByHash",
        "hmy_getBlockTransactionCountByHash",
        "hmy_getTransactionByHash",
        "hmy_getTransactionByBlockHashAndIndex",
        "hmy_getBlockByHash",
        "hmy_getStakingTransactionByBlockNumberAndIndex",
        "hmy_getTransactionReceipt",
        "hmy_getBlockTransactionCountByNumber",
        "hmy_getTransactionByBlockNumberAndIndex",
        "hmy_getBlockByNumber",
        "hmy_getAllValidatorInformationByBlockNumber",
        "hmyv2_getStakingTransactionByBlockHashAndIndex",
        "hmyv2_getStakingTransactionByBlockNumberAndIndex",
        "hmyv2_getStakingTransactionByHash",
        "hmyv2_getTransactionReceipt",
        "hmyv2_getBlockTransactionCountByHash",
        "hmyv2_getBlockTransactionCountByNumber",
        "hmyv2_getTransactionByHash",
        "hmyv2_getTransactionByBlockNumberAndIndex",
        "hmyv2_getTransactionByBlockHashAndIndex",
        "hmyv2_getBlockByNumber",
        "hmyv2_getBlockByHash",
        "hmyv2_getCXReceiptByHash",
    )

    private val klayPossibleNotIndexedMethods = listOf(
        "klay_blockNumber",
        "klay_getBlockByHash",
        "klay_getBlockReceipts",
        "klay_getBlockTransactionCountByNumber",
        "klay_getBlockWithConsensusInfoByNumber",
        "klay_getBlockByNumber",
        "klay_getBlockTransactionCountByHash",
        "klay_getHeaderByNumber",
        "klay_getHeaderByHash",
        "klay_getBlockWithConsensusInfoByNumberRange",
        "klay_getBlockWithConsensusInfoByHash",
        "klay_getDecodedAnchoringTransactionByHash",
        "klay_getRawTransactionByBlockNumberAndIndex",
        "klay_getRawTransactionByBlockHashAndIndex",
        "klay_getRawTransactionByHash",
        "klay_getTransactionByBlockNumberAndIndex",
        "klay_getTransactionBySenderTxHash",
        "klay_getTransactionByBlockHashAndIndex",
        "klay_getTransactionByHash",
        "klay_getTransactionReceipt",
        "klay_getTransactionReceiptBySenderTxHash",
    )

    private val kaiaPossibleNotIndexedMethods = listOf(
        "kaia_blockNumber",
        "kaia_getBlockByHash",
        "kaia_getBlockReceipts",
        "kaia_getBlockTransactionCountByNumber",
        "kaia_getBlockWithConsensusInfoByNumber",
        "kaia_getBlockByNumber",
        "kaia_getBlockTransactionCountByHash",
        "kaia_getHeaderByNumber",
        "kaia_getHeaderByHash",
        "kaia_getBlockWithConsensusInfoByNumberRange",
        "kaia_getBlockWithConsensusInfoByHash",
        "kaia_getDecodedAnchoringTransactionByHash",
        "kaia_getRawTransactionByBlockNumberAndIndex",
        "kaia_getRawTransactionByBlockHashAndIndex",
        "kaia_getRawTransactionByHash",
        "kaia_getTransactionByBlockNumberAndIndex",
        "kaia_getTransactionBySenderTxHash",
        "kaia_getTransactionByBlockHashAndIndex",
        "kaia_getTransactionByHash",
        "kaia_getTransactionReceipt",
        "kaia_getTransactionReceiptBySenderTxHash",
    )

    private val firstValueMethods = listOf(
        "eth_call",
        "eth_getStorageAt",
        "eth_getCode",
        "eth_getLogs",
        "eth_maxPriorityFeePerGas",
        "eth_getProof",
        "eth_createAccessList",
        "eth_getBlockReceipts",
    )

    private val specialMethods = listOf(
        "eth_getTransactionCount",
        "eth_blockNumber",
        "eth_getBalance",
        "eth_sendRawTransaction",
    )

    private val harmonySpecialMethods = listOf(
        "hmy_sendRawStakingTransaction",
        "hmy_sendRawTransaction",
        "hmy_getTransactionCount",
    )

    private val klaySpecialMethods = listOf(
        "klay_sendRawTransaction",
        "klay_getTransactionCount",
    )

    private val kaiaSpecialMethods = listOf(
        "kaia_sendRawTransaction",
        "kaia_getTransactionCount",
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

    private val filecoinMethods = listOf(
        "Filecoin.ChainBlockstoreInfo",
        "Filecoin.ChainExport",
        "Filecoin.ChainGetBlock",
        "Filecoin.ChainGetBlockMessages",
        "Filecoin.ChainGetEvents",
        "Filecoin.ChainGetGenesis",
        "Filecoin.ChainGetMessage",
        "Filecoin.ChainGetMessagesInTipset",
        "Filecoin.ChainGetNode",
        "Filecoin.ChainGetParentMessages",
        "Filecoin.ChainGetParentReceipts",
        "Filecoin.ChainGetPath",
        "Filecoin.ChainGetTipSet",
        "Filecoin.ChainGetTipSetAfterHeight",
        "Filecoin.ChainGetTipSetByHeight",
        "Filecoin.ChainHasObj",
        "Filecoin.ChainHead",
        "Filecoin.ChainHotGC",
        "Filecoin.ChainNotify",
        "Filecoin.ChainReadObj",
        "Filecoin.ChainStatObj",
        "Filecoin.ChainTipSetWeight",
        "Filecoin.ClientDealPieceCID",
        "Filecoin.ClientDealSize",
        "Filecoin.ClientFindData",
        "Filecoin.ClientGetDealInfo",
        "Filecoin.ClientGetDealStatus",
        "Filecoin.ClientMinerQueryOffer",
        "Filecoin.ClientQueryAsk",
        "Filecoin.GasEstimateFeeCap",
        "Filecoin.GasEstimateGasLimit",
        "Filecoin.GasEstimateGasPremium",
        "Filecoin.GasEstimateMessageGas",
        "Filecoin.ID",
        "Filecoin.MinerGetBaseInfo",
        "Filecoin.MpoolCheckMessages",
        "Filecoin.MpoolCheckPendingMessages",
        "Filecoin.MpoolCheckReplaceMessages",
        "Filecoin.MpoolGetConfig",
        "Filecoin.MpoolGetNonce",
        "Filecoin.MpoolPending",
        "Filecoin.MpoolPush",
        "Filecoin.MpoolSelect",
        "Filecoin.MpoolSub",
        "Filecoin.MsigGetAvailableBalance",
        "Filecoin.MsigGetPending",
        "Filecoin.MsigGetVested",
        "Filecoin.MsigGetVestingSchedule",
        "Filecoin.NetAddrsListen",
        "Filecoin.NetAgentVersion",
        "Filecoin.NetAutoNatStatus",
        "Filecoin.NetBandwidthStats",
        "Filecoin.NetBandwidthStatsByPeer",
        "Filecoin.NetBandwidthStatsByProtocol",
        "Filecoin.NetBlockList",
        "Filecoin.NetConnectedness",
        "Filecoin.NetFindPeer",
        "Filecoin.NetLimit",
        "Filecoin.NetListening",
        "Filecoin.NetPeerInfo",
        "Filecoin.NetPeers",
        "Filecoin.NetPing",
        "Filecoin.NetProtectList",
        "Filecoin.NetPubsubScores",
        "Filecoin.NetStat",
        "Filecoin.NetVersion",
        "Filecoin.NodeStatus",
        "Filecoin.PaychList",
        "Filecoin.PaychStatus",
        "Filecoin.PaychVoucherCheckSpendable",
        "Filecoin.PaychVoucherCheckValid",
        "Filecoin.RaftLeader",
        "Filecoin.RaftState",
        "Filecoin.StartTime",
        "Filecoin.StateAccountKey",
        "Filecoin.StateActorCodeCIDs",
        "Filecoin.StateActorManifestCID",
        "Filecoin.StateAllMinerFaults",
        "Filecoin.StateCall",
        "Filecoin.StateChangedActors",
        "Filecoin.StateCirculatingSupply",
        "Filecoin.StateCompute",
        "Filecoin.StateComputeDataCID",
        "Filecoin.StateDealProviderCollateralBounds",
        "Filecoin.StateDecodeParams",
        "Filecoin.StateEncodeParams",
        "Filecoin.StateGetActor",
        "Filecoin.StateGetAllocation",
        "Filecoin.StateGetAllocationForPendingDeal",
        "Filecoin.StateGetAllocations",
        "Filecoin.StateGetBeaconEntry",
        "Filecoin.StateGetClaim",
        "Filecoin.StateGetClaims",
        "Filecoin.StateGetNetworkParams",
        "Filecoin.StateGetRandomnessDigestFromBeacon",
        "Filecoin.StateGetRandomnessDigestFromTickets",
        "Filecoin.StateGetRandomnessFromBeacon",
        "Filecoin.StateGetRandomnessFromTickets",
        "Filecoin.StateGetReceipt",
        "Filecoin.StateListActors",
        "Filecoin.StateListMessages",
        "Filecoin.StateListMiners",
        "Filecoin.StateLookupID",
        "Filecoin.StateLookupRobustAddress",
        "Filecoin.StateMarketBalance",
        "Filecoin.StateMarketDeals",
        "Filecoin.StateMarketParticipants",
        "Filecoin.StateMarketStorageDeal",
        "Filecoin.StateMinerActiveSectors",
        "Filecoin.StateMinerAllocated",
        "Filecoin.StateMinerAvailableBalance",
        "Filecoin.StateMinerDeadlines",
        "Filecoin.StateMinerFaults",
        "Filecoin.StateMinerInfo",
        "Filecoin.StateMinerInitialPledgeCollateral",
        "Filecoin.StateMinerPartitions",
        "Filecoin.StateMinerPower",
        "Filecoin.StateMinerPreCommitDepositForPower",
        "Filecoin.StateMinerProvingDeadline",
        "Filecoin.StateMinerRecoveries",
        "Filecoin.StateMinerSectorAllocated",
        "Filecoin.StateMinerSectorCount",
        "Filecoin.StateMinerSectors",
        "Filecoin.StateNetworkName",
        "Filecoin.StateNetworkVersion",
        "Filecoin.StateReadState",
        "Filecoin.StateReplay",
        "Filecoin.StateSearchMsg",
        "Filecoin.StateSearchMsgLimited",
        "Filecoin.StateSectorExpiration",
        "Filecoin.StateSectorGetInfo",
        "Filecoin.StateSectorPartition",
        "Filecoin.StateSectorPreCommitInfo",
        "Filecoin.StateVerifiedClientStatus",
        "Filecoin.StateVerifiedRegistryRootKey",
        "Filecoin.StateVerifierStatus",
        "Filecoin.StateVMCirculatingSupplyInternal",
        "Filecoin.StateWaitMsg",
        "Filecoin.StateWaitMsgLimited",
        "Filecoin.SyncCheckBad",
        "Filecoin.SyncIncomingBlocks",
        "Filecoin.SyncState",
        "Filecoin.SyncValidateTipset",
        "Filecoin.Version",
        "Filecoin.WalletBalance",
        "Filecoin.WalletValidateAddress",
        "Filecoin.WalletVerify",
        "Filecoin.Web3ClientVersion",
    )

    private val klayMethods = listOf(
        "klay_accountCreated",
        "klay_accounts",
        "klay_decodeAccountKey",
        "klay_getAccountKey",
        "klay_getCode",
        "klay_encodeAccountKey",
        "klay_getAccount",
        "klay_getAccount",
        "klay_sign",
        "klay_isContractAccount",

        "klay_getCommittee",
        "klay_getCommitteeSize",
        "klay_getCouncil",
        "klay_getCouncilSize",

        "klay_getRewards",
        "klay_getStorageAt",
        "klay_syncing",

        "klay_call",

        "klay_estimateGas",

        "klay_estimateComputationCost",
        "klay_pendingTransactions",
        "klay_createAccessList",

        "klay_resend",

        "klay_chainID",
        "klay_clientVersion",
        "klay_gasPriceAt",
        "klay_gasPrice",
        "klay_protocolVersion",
        "klay_getChainConfig",
        "klay_forkStatus",
        "klay_getFilterChanges",
        "klay_getFilterLogs",
        "klay_newBlockFilter",
        "klay_newPendingTransactionFilter",
        "klay_uninstallFilter",
        "klay_unsubscribe",
        "klay_getLogs",
        "klay_subscribe",
        "klay_newFilter",

        "klay_feeHistory",
        "klay_lowerBoundGasPrice",
        "klay_upperBoundGasPrice",
        "klay_maxPriorityFeePerGas",

        "klay_getStakingInfo",
        "klay_sha3",
        "klay_recoverFromTransaction",
        "klay_recoverFromMessage",
        "klay_getProof",
        "klay_nodeAddress",

        // they exist, but i have doubts that we need to expose them
        // "klay_rewardbase"
        // "klay_isParallelDBWrite"
        // "klay_isSenderTxHashIndexingEnabled"
    )

    private val seiMethods = listOf(
        "sei_getCosmosTx",
        "sei_getEvmTx",
        "sei_getEVMAddress",
        "sei_getSeiAddress",
    )

    private val kaiaMethods = listOf(
        "kaia_accountCreated",
        "kaia_accounts",
        "kaia_decodeAccountKey",
        "kaia_getAccountKey",
        "kaia_getCode",
        "kaia_encodeAccountKey",
        "kaia_getAccount",
        "kaia_getAccount",
        "kaia_sign",
        "kaia_isContractAccount",

        "kaia_getCommittee",
        "kaia_getCommitteeSize",
        "kaia_getCouncil",
        "kaia_getCouncilSize",

        "kaia_getRewards",
        "kaia_getStorageAt",
        "kaia_syncing",

        "kaia_call",

        "kaia_estimateGas",

        "kaia_estimateComputationCost",
        "kaia_pendingTransactions",
        "kaia_createAccessList",

        "kaia_resend",

        "kaia_chainID",
        "kaia_clientVersion",
        "kaia_gasPriceAt",
        "kaia_gasPrice",
        "kaia_protocolVersion",
        "kaia_getChainConfig",
        "kaia_forkStatus",
        "kaia_getFilterChanges",
        "kaia_getFilterLogs",
        "kaia_newBlockFilter",
        "kaia_newPendingTransactionFilter",
        "kaia_uninstallFilter",
        "kaia_unsubscribe",
        "kaia_getLogs",
        "kaia_subscribe",
        "kaia_newFilter",

        "kaia_feeHistory",
        "kaia_lowerBoundGasPrice",
        "kaia_upperBoundGasPrice",
        "kaia_maxPriorityFeePerGas",

        "kaia_getStakingInfo",
        "kaia_sha3",
        "kaia_recoverFromTransaction",
        "kaia_recoverFromMessage",
        "kaia_getProof",
        "kaia_nodeAddress",

        // they exist, but i have doubts that we need to expose them
        // "kaia_rewardbase"
        // "kaia_isParallelDBWrite"
        // "kaia_isSenderTxHashIndexingEnabled"
    )

    private val harmonyMethods = listOf(
        "hmy_newFilter",
        "hmy_newBlockFilter",
        "hmy_newPendingTransactionFilter",
        "hmy_getFilterLogs",
        "hmy_getFilterChanges",
        "hmy_getPendingCrossLinks",
        "hmy_getCurrentTransactionErrorSink",
        "hmy_getPendingCXReceipts",
        "hmy_pendingTransactions",
        "hmy_getTransactionsHistory",
        "hmy_getBlocks",
        "hmy_getBalanceByBlockNumber",
        "hmy_getBalance",
        "hmy_getLogs",
        "hmy_estimateGas",
        "hmy_getStorageAt",
        "hmy_call",
        "hmy_getCode",
        "hmy_isLastBlock",
        "hmy_latestHeader",
        "hmy_blockNumber",
        "hmy_gasPrice",
        "hmy_getEpoch",
        "hmy_epochLastBlock",
        "hmy_getShardingStructure",
        "hmy_syncing",
        "hmy_getLeader",
        "hmy_getCirculatingSupply",
        "hmy_getTotalSupply",
        "hmy_getStakingNetworkInfo",
        "hmy_getAllValidatorInformation",
        "hmy_getDelegationsByValidator",
        "hmy_getDelegationsByDelegatorAndValidator",
        "hmy_getDelegationsByDelegator",
        "hmy_getValidatorMetrics",
        "hmy_getMedianRawStakeSnapshot",
        "hmy_getElectedValidatorAddresses",
        "hmy_getAllValidatorAddresses",
        "hmy_getCurrentStakingErrorSink",
        "hmy_getValidatorInformation",
        "hmy_getSignedBlocks",
        "hmy_getValidators",
        "hmy_isBlockSigner",
        "hmy_getBlockSigners",
        "hmyv2_getBalanceByBlockNumber",
        "hmyv2_getTransactionCount",
        "hmyv2_getBalance",
        "hmyv2_getCurrentTransactionErrorSink",
        "hmyv2_getPendingCrossLinks",
        "hmyv2_getPendingCXReceipts",
        "hmyv2_pendingTransactions",
        "hmyv2_getTransactionsHistory",
        "hmyv2_getBlocks",
        "hmyv2_blockNumber",
        "hmyv2_gasPrice",
        "hmyv2_getEpoch",
        "hmyv2_getValidators",
    )

    private val zxkSyncMethods = listOf(
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
            possibleNotIndexedMethods.contains(method) || harmonyPossibleNotIndexedMethods.contains(method) || klayPossibleNotIndexedMethods.contains(method) || kaiaPossibleNotIndexedMethods.contains(method) -> NotNullQuorum()
            specialMethods.contains(method) || harmonySpecialMethods.contains(method) || klaySpecialMethods.contains(method) || kaiaSpecialMethods.contains(method) -> {
                when (method) {
                    "eth_getTransactionCount", "hmy_getTransactionCount", "klay_getTransactionCount", "kaia_getTransactionCount" -> MaximumValueQuorum()
                    "eth_sendRawTransaction", "hmy_sendRawStakingTransaction", "hmy_sendRawTransaction", "klay_sendRawTransaction", "kaia_sendRawTransaction", "Filecoin.MpoolPush" -> BroadcastQuorum()
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
            Chain.OPTIMISM__MAINNET, Chain.OPTIMISM__SEPOLIA ->
                listOf(
                    "optimism_outputAtBlock",
                    "optimism_syncStatus",
                    "optimism_rollupConfig",
                    "optimism_version",
                    "rollup_gasPrices",
                )
            Chain.SCROLL__MAINNET, Chain.SCROLL__SEPOLIA ->
                listOf(
                    "scroll_estimateL1DataFee",
                )
            Chain.KLAYTN__MAINNET, Chain.KLAYTN__BAOBAB ->
                klayMethods
                    .plus(klaySpecialMethods)
                    .plus(klayPossibleNotIndexedMethods)
                    .plus(kaiaMethods)
                    .plus(kaiaSpecialMethods)
                    .plus(kaiaPossibleNotIndexedMethods)
            Chain.MANTLE__MAINNET, Chain.MANTLE__SEPOLIA ->
                listOf(
                    "rollup_gasPrices",
                    "eth_getBlockRange",
                )
            Chain.POLYGON__MAINNET -> listOf(
                "bor_getAuthor",
                "bor_getCurrentValidators",
                "bor_getCurrentProposer",
                "bor_getRootHash",
                "bor_getSignersAtHash",
                "eth_getRootHash",
            )

            Chain.POLYGON_ZKEVM__MAINNET, Chain.POLYGON_ZKEVM__CARDONA -> listOf(
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

            Chain.ZKSYNC__MAINNET, Chain.ZKSYNC__SEPOLIA -> zxkSyncMethods

            Chain.HARMONY__MAINNET_SHARD_0 ->
                harmonyMethods
                    .plus(harmonySpecialMethods)
                    .plus(harmonyPossibleNotIndexedMethods)
                    .plus(
                        listOf(
                            "hmy_getCurrentUtilityMetrics",
                        ),
                    )

            Chain.HARMONY__MAINNET_SHARD_1 ->
                harmonyMethods
                    .plus(harmonySpecialMethods)
                    .plus(harmonyPossibleNotIndexedMethods)

            Chain.LINEA__MAINNET, Chain.LINEA__SEPOLIA -> listOf(
                "linea_estimateGas",
            )

            Chain.ROOTSTOCK__MAINNET, Chain.ROOTSTOCK__TESTNET -> listOf(
                "rsk_getRawTransactionReceiptByHash",
                "rsk_getTransactionReceiptNodesByHash",
                "rsk_getRawBlockHeaderByHash",
                "rsk_getRawBlockHeaderByNumber",
                "rsk_protocolVersion",
            )
            Chain.TRON__MAINNET, Chain.TRON__SHASTA -> listOf(
                "buildTransaction",
            )

            Chain.FILECOIN__MAINNET, Chain.FILECOIN__CALIBRATION_TESTNET -> filecoinMethods

            Chain.SEI__MAINNET, Chain.SEI__TESTNET, Chain.SEI__DEVNET -> seiMethods

            Chain.CRONOS_ZKEVM__MAINNET, Chain.CRONOS_ZKEVM__TESTNET -> zxkSyncMethods + "zk_estimateFee"

            Chain.VICTION__MAINNET, Chain.VICTION__TESTNET -> listOf(
                "posv_getNetworkInformation",
            )

            else -> emptyList()
        }
    }

    private fun chainUnsupportedMethods(chain: Chain): Set<String> {
        return when (chain) {
            Chain.OPTIMISM__MAINNET -> setOf("eth_getAccounts")
            Chain.ZKSYNC__MAINNET, Chain.POLYGON_ZKEVM__MAINNET ->
                setOf("eth_maxPriorityFeePerGas", "zks_getBytecodeByHash")
            Chain.TELOS__MAINNET, Chain.TELOS__TESTNET -> setOf(
                "eth_syncing",
                "net_peerCount",
            )
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

    /* Arbitrum note: all arbtrace_ methods can be used on blocks prior to 22207816,
     while debug_trace methods can be used for blocks after 22207818.
     Block 22207817 cannot be traced but is empty.
     */
    private fun getTraceMethods(): List<String> {
        return when (chain) {
            Chain.ARBITRUM__MAINNET -> arbitrumTraceMethods
            else -> traceMethods
        }
    }

    override fun getGroupMethods(groupName: String): Set<String> =
        when (groupName) {
            "filter" -> filterMethods
            "trace" -> getTraceMethods()
            "debug" -> debugMethods
            "default" -> getSupportedMethods()
            else -> emptyList()
        }.toSet()

    override fun getSupportedMethods(): Set<String> {
        return allowedMethods.plus(hardcodedMethods).toSortedSet()
    }

    fun getAllMethods(): Set<String> =
        getSupportedMethods()
            .plus(getGroupMethods("filter"))
            .plus(getGroupMethods("trace"))
            .plus(getGroupMethods("debug"))
            .plus(getChainSpecificMethods(chain))
            .toSet()
}
