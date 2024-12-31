package io.emeraldpay.dshackle.upstream.calls

import io.emeraldpay.dshackle.quorum.AlwaysQuorum
import io.emeraldpay.dshackle.quorum.CallQuorum
import io.emeraldpay.dshackle.upstream.ethereum.rpc.RpcException

class DefaultBeaconChainMethods : CallMethods {

    private val beaconMethods = setOf(
        getMethod("/eth/v1/beacon/genesis"),
        getMethod("/eth/v1/beacon/states/*/root"),
        getMethod("/eth/v1/beacon/states/*/fork"),
        getMethod("/eth/v1/beacon/states/*/finality_checkpoints"),
        // getMethod("/eth/v1/beacon/states/*/validators"), // RESPONSE TOO BIG
        postMethod("/eth/v1/beacon/states/*/validators"),
        getMethod("/eth/v1/beacon/states/*/validators/*"),
        // getMethod("/eth/v1/beacon/states/*/validator_balances"), // RESPONSE TOO BIG
        postMethod("/eth/v1/beacon/states/*/validator_balances"),
        getMethod("/eth/v1/beacon/states/*/committees"),
        getMethod("/eth/v1/beacon/states/*/sync_committees"),
        getMethod("/eth/v1/beacon/states/*/randao"),
        getMethod("/eth/v1/beacon/headers"),
        getMethod("/eth/v1/beacon/headers/*"),
        postMethod("/eth/v1/beacon/blinded_blocks"),
        postMethod("/eth/v2/beacon/blinded_blocks"),
        postMethod("/eth/v1/beacon/blocks"),
        postMethod("/eth/v2/beacon/blocks"),
        getMethod("/eth/v2/beacon/blocks/*"),
        getMethod("/eth/v1/beacon/blocks/*/root"),
        getMethod("/eth/v1/beacon/blocks/*/attestations"),
        getMethod("/eth/v1/beacon/blob_sidecars/*"),
        postMethod("/eth/v1/beacon/rewards/sync_committee/*"),
        getMethod("/eth/v1/beacon/deposit_snapshot"),
        getMethod("/eth/v1/beacon/rewards/blocks/*"),
        postMethod("/eth/v1/beacon/rewards/attestations/*"),
        getMethod("/eth/v1/beacon/blinded_blocks/*"),
        getMethod("/eth/v1/beacon/light_client/bootstrap/*"),
        getMethod("/eth/v1/beacon/light_client/updates"),
        getMethod("/eth/v1/beacon/light_client/finality_update"),
        getMethod("/eth/v1/beacon/light_client/optimistic_update"),
        getMethod("/eth/v1/beacon/pool/attestations"),
        postMethod("/eth/v1/beacon/pool/attestations"),
        getMethod("/eth/v1/beacon/pool/attester_slashings"),
        postMethod("/eth/v1/beacon/pool/attester_slashings"),
        getMethod("/eth/v1/beacon/pool/proposer_slashings"),
        postMethod("/eth/v1/beacon/pool/proposer_slashings"),
        postMethod("/eth/v1/beacon/pool/sync_committees"),
        getMethod("/eth/v1/beacon/pool/voluntary_exits"),
        postMethod("/eth/v1/beacon/pool/voluntary_exits"),
        getMethod("/eth/v1/beacon/pool/bls_to_execution_changes"),
        postMethod("/eth/v1/beacon/pool/bls_to_execution_changes"),
    )

    private val builderMethods = setOf(
        getMethod("/eth/v1/builder/states/*/expected_withdrawals"),
    )

    private val configMethods = setOf(
        getMethod("/eth/v1/config/fork_schedule"),
        getMethod("/eth/v1/config/spec"),
        getMethod("/eth/v1/config/deposit_contract"),
    )

    private val debugMethods = setOf(
        getMethod("/eth/v2/debug/beacon/states/*"),
        getMethod("/eth/v2/debug/beacon/heads"),
        getMethod("/eth/v1/debug/fork_choice"),
    )

//    not supported
//    private val eventMethods = setOf(
//        getMethod("/eth/v1/events"),
//    )

    // need to think up what to do with these methods, probably hardcode them?
    private val nodeMethods = setOf(
        getMethod("/eth/v1/node/identity"),
        getMethod("/eth/v1/node/peers"),
        getMethod("/eth/v1/node/peers/*"),
        getMethod("/eth/v1/node/peer_count"),
        getMethod("/eth/v1/node/version"),
        getMethod("/eth/v1/node/syncing"),
        getMethod("/eth/v1/node/health"),
    )

    private val validatorMethods = setOf(
        postMethod("/eth/v1/validator/duties/attester/*"),
        getMethod("/eth/v1/validator/duties/proposer/*"),
        postMethod("/eth/v1/validator/duties/sync/*"),
        getMethod("/eth/v3/validator/blocks/*"),
        getMethod("/eth/v1/validator/attestation_data"),
        getMethod("/eth/v1/validator/aggregate_attestation"),
        postMethod("/eth/v1/validator/aggregate_and_proofs"),
        postMethod("/eth/v1/validator/beacon_committee_subscriptions"),
        postMethod("/eth/v1/validator/sync_committee_subscriptions"),
        postMethod("/eth/v1/validator/sync_committee_subscriptions"),
        getMethod("/eth/v1/validator/sync_committee_contribution"),
        postMethod("/eth/v1/validator/sync_committee_selections"),
        postMethod("/eth/v1/validator/contribution_and_proofs"),
        postMethod("/eth/v1/validator/prepare_beacon_proposer"),
        postMethod("/eth/v1/validator/register_validator"),
        postMethod("/eth/v1/validator/liveness/*"),
    )

    private val rewardMethods = setOf(
        postMethod("/eth/v1/beacon/rewards/sync_committee/*"),
        getMethod("/eth/v1/beacon/rewards/blocks/*"),
        postMethod("/eth/v1/beacon/rewards/blocks/*"),
    )

    private val allowedMethods: Set<String> =
        beaconMethods + builderMethods + configMethods + debugMethods + nodeMethods + validatorMethods + rewardMethods

    override fun createQuorumFor(method: String): CallQuorum {
        return AlwaysQuorum()
    }

    override fun isCallable(method: String): Boolean {
        return allowedMethods.contains(method)
    }

    override fun getSupportedMethods(): Set<String> {
        return allowedMethods.toSortedSet()
    }

    override fun isHardcoded(method: String): Boolean {
        return false
    }

    override fun executeHardcoded(method: String): ByteArray {
        throw RpcException(-32601, "Method not found")
    }

    override fun getGroupMethods(groupName: String): Set<String> {
        return when (groupName) {
            "default" -> getSupportedMethods()
            else -> emptyList()
        }.toSet()
    }

    private fun getMethod(method: String) = "GET#$method"

    private fun postMethod(method: String) = "POST#$method"
}
