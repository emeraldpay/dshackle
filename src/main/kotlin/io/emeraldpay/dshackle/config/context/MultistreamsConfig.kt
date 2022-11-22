package io.emeraldpay.dshackle.config.context

import io.emeraldpay.dshackle.BlockchainType
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.cache.CachesFactory
import io.emeraldpay.dshackle.upstream.CallTargetsHolder
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.bitcoin.BitcoinMultistream
import io.emeraldpay.dshackle.upstream.ethereum.EthereumMultistream
import io.emeraldpay.dshackle.upstream.ethereum.EthereumPosMultiStream
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
open class MultistreamsConfig(val beanFactory: ConfigurableListableBeanFactory) {
    @Bean
    open fun allMultistreams(
        cachesFactory: CachesFactory,
        callTargetsHolder: CallTargetsHolder
    ): List<Multistream> {
        return Chain.values()
            .filterNot { it == Chain.UNSPECIFIED }
            .mapNotNull { chain ->
                when (BlockchainType.from(chain)) {
                    BlockchainType.EVM_POS -> ethereumPosMultistream(chain, cachesFactory)
                    BlockchainType.EVM_POW -> ethereumMultistream(chain, cachesFactory)
                    BlockchainType.BITCOIN -> bitcoinMultistream(chain, cachesFactory)
                    else -> null
                }
            }
    }

    private fun ethereumMultistream(
        chain: Chain,
        cachesFactory: CachesFactory
    ): EthereumMultistream {
        val name = "multi-ethereum-$chain"

        return EthereumMultistream(
            chain,
            ArrayList(),
            cachesFactory.getCaches(chain)
        ).also { register(it, name) }
    }

    open fun ethereumPosMultistream(
        chain: Chain,
        cachesFactory: CachesFactory
    ): EthereumPosMultiStream {
        val name = "multi-ethereum-pos-$chain"

        return EthereumPosMultiStream(
            chain,
            ArrayList(),
            cachesFactory.getCaches(chain)
        ).also { register(it, name) }
    }

    open fun bitcoinMultistream(
        chain: Chain,
        cachesFactory: CachesFactory
    ): BitcoinMultistream {
        val name = "multi-bitcoin-$chain"

        return BitcoinMultistream(
            chain,
            ArrayList(),
            cachesFactory.getCaches(chain)
        ).also { register(it, name) }
    }

    private fun register(bean: Any, name: String) {
        beanFactory.initializeBean(bean, name)
        beanFactory.autowireBean(bean)
        this.beanFactory.registerSingleton(name, bean)
    }
}
