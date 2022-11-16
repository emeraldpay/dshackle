package io.emeraldpay.dshackle.config.context

import io.emeraldpay.dshackle.cache.CachesFactory
import io.emeraldpay.dshackle.upstream.CallTargetsHolder
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.bitcoin.BitcoinMultistream
import io.emeraldpay.dshackle.upstream.ethereum.EthereumMultistream
import io.emeraldpay.dshackle.upstream.ethereum.EthereumPosMultiStream
import io.emeraldpay.grpc.BlockchainType
import io.emeraldpay.grpc.Chain
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class MultistreamsConfig {
    @Bean
    fun allMultistreams(
        cachesFactory: CachesFactory,
        callTargetsHolder: CallTargetsHolder
    ): List<Multistream> {
        return Chain.values()
            .mapNotNull { chain ->
                when (BlockchainType.from(chain)) {
                    BlockchainType.EVM_POS -> EthereumPosMultiStream(
                        chain,
                        ArrayList(),
                        cachesFactory.getCaches(chain),
                        callTargetsHolder
                    )
                    BlockchainType.EVM_POW -> EthereumMultistream(
                        chain,
                        ArrayList(),
                        cachesFactory.getCaches(chain),
                        callTargetsHolder
                    )
                    BlockchainType.BITCOIN -> BitcoinMultistream(
                        chain,
                        ArrayList(),
                        cachesFactory.getCaches(chain),
                        callTargetsHolder
                    )
                    else -> null
                }
            }
    }
}
