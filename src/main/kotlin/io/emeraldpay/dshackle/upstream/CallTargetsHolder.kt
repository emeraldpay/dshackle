package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.BlockchainType
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.calls.DefaultBitcoinMethods
import io.emeraldpay.dshackle.upstream.calls.DefaultEthereumMethods
import io.emeraldpay.dshackle.upstream.calls.DefaultStarknetMethods
import org.springframework.stereotype.Component

@Component
class CallTargetsHolder {
    private val callTargets = HashMap<Chain, CallMethods>()

    fun getDefaultMethods(chain: Chain): CallMethods {
        return callTargets[chain] ?: return setupDefaultMethods(chain)
    }

    private fun setupDefaultMethods(chain: Chain): CallMethods {
        val created = when (BlockchainType.from(chain)) {
            BlockchainType.BITCOIN -> DefaultBitcoinMethods()
            BlockchainType.ETHEREUM -> DefaultEthereumMethods(chain)
            BlockchainType.STARKNET -> DefaultStarknetMethods(chain)
        }
        callTargets[chain] = created
        return created
    }
}
