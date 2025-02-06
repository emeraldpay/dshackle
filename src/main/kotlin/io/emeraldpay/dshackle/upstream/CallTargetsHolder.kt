package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.BlockchainType.BITCOIN
import io.emeraldpay.dshackle.BlockchainType.COSMOS
import io.emeraldpay.dshackle.BlockchainType.ETHEREUM
import io.emeraldpay.dshackle.BlockchainType.ETHEREUM_BEACON_CHAIN
import io.emeraldpay.dshackle.BlockchainType.NEAR
import io.emeraldpay.dshackle.BlockchainType.POLKADOT
import io.emeraldpay.dshackle.BlockchainType.SOLANA
import io.emeraldpay.dshackle.BlockchainType.STARKNET
import io.emeraldpay.dshackle.BlockchainType.TON
import io.emeraldpay.dshackle.BlockchainType.UNKNOWN
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.foundation.ChainOptions
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.calls.DefaultBeaconChainMethods
import io.emeraldpay.dshackle.upstream.calls.DefaultBitcoinMethods
import io.emeraldpay.dshackle.upstream.calls.DefaultCosmosMethods
import io.emeraldpay.dshackle.upstream.calls.DefaultEthereumMethods
import io.emeraldpay.dshackle.upstream.calls.DefaultNearMethods
import io.emeraldpay.dshackle.upstream.calls.DefaultPolkadotMethods
import io.emeraldpay.dshackle.upstream.calls.DefaultStarknetMethods
import io.emeraldpay.dshackle.upstream.calls.DefaultTonHttpMethods
import org.springframework.stereotype.Component

@Component
class CallTargetsHolder {
    private val callTargets = HashMap<Chain, CallMethods>()

    fun getDefaultMethods(
        chain: Chain,
        options: ChainOptions.Options,
        connection: UpstreamsConfig.UpstreamConnection?,
    ): CallMethods {
        return callTargets[chain] ?: return setupDefaultMethods(chain, options, connection)
    }

    private fun setupDefaultMethods(
        chain: Chain,
        options: ChainOptions.Options,
        connection: UpstreamsConfig.UpstreamConnection?,
    ): CallMethods {
        val created = when (chain.type) {
            BITCOIN -> DefaultBitcoinMethods(options.providesBalance == true)
            ETHEREUM -> DefaultEthereumMethods(chain)
            STARKNET -> DefaultStarknetMethods(chain)
            POLKADOT -> DefaultPolkadotMethods(chain)
            SOLANA -> DefaultSolanaMethods()
            NEAR -> DefaultNearMethods()
            ETHEREUM_BEACON_CHAIN -> DefaultBeaconChainMethods()
            COSMOS -> DefaultCosmosMethods()
            TON -> DefaultTonHttpMethods(connection)
            UNKNOWN -> throw IllegalArgumentException("unknown chain")
        }
        callTargets[chain] = created
        return created
    }
}
