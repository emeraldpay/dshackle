package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.BlockchainType.BITCOIN
import io.emeraldpay.dshackle.BlockchainType.ETHEREUM
import io.emeraldpay.dshackle.BlockchainType.POLKADOT
import io.emeraldpay.dshackle.BlockchainType.STARKNET
import io.emeraldpay.dshackle.BlockchainType.UNKNOWN
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.calls.DefaultBitcoinMethods
import io.emeraldpay.dshackle.upstream.calls.DefaultEthereumMethods
import io.emeraldpay.dshackle.upstream.calls.DefaultPolkadotMethods
import io.emeraldpay.dshackle.upstream.calls.DefaultStarknetMethods
import org.springframework.stereotype.Component

@Component
class CallTargetsHolder {
    private val callTargets = HashMap<Chain, CallMethods>()

    fun getDefaultMethods(chain: Chain): CallMethods {
        return callTargets[chain] ?: return setupDefaultMethods(chain)
    }

    private fun setupDefaultMethods(chain: Chain): CallMethods {
        val created = when (chain.type) {
            BITCOIN -> DefaultBitcoinMethods()
            ETHEREUM -> DefaultEthereumMethods(chain)
            STARKNET -> DefaultStarknetMethods(chain)
            POLKADOT -> DefaultPolkadotMethods()
            UNKNOWN -> throw IllegalArgumentException("unknown chain")
        }
        callTargets[chain] = created
        return created
    }
}
