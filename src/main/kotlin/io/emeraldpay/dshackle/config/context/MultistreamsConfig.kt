package io.emeraldpay.dshackle.config.context

import io.emeraldpay.dshackle.BlockchainType
import io.emeraldpay.dshackle.BlockchainType.BITCOIN
import io.emeraldpay.dshackle.BlockchainType.EVM_POS
import io.emeraldpay.dshackle.BlockchainType.EVM_POW
import io.emeraldpay.dshackle.BlockchainType.STARKNET
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.cache.CachesFactory
import io.emeraldpay.dshackle.config.IndexConfig
import io.emeraldpay.dshackle.upstream.CallTargetsHolder
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.bitcoin.BitcoinMultistream
import io.emeraldpay.dshackle.upstream.ethereum.EthereumMultistream
import io.emeraldpay.dshackle.upstream.ethereum.EthereumPosMultiStream
import io.emeraldpay.dshackle.upstream.generic.GenericMultistream
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory
import org.springframework.cloud.sleuth.Tracer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import reactor.core.scheduler.Scheduler
import java.util.concurrent.CopyOnWriteArrayList

@Configuration
open class MultistreamsConfig(val beanFactory: ConfigurableListableBeanFactory) {
    @Bean
    open fun allMultistreams(
        cachesFactory: CachesFactory,
        callTargetsHolder: CallTargetsHolder,
        @Qualifier("headScheduler")
        headScheduler: Scheduler,
        tracer: Tracer,
        indexConfig: IndexConfig,
    ): List<Multistream> {
        return Chain.values()
            .filterNot { it == Chain.UNSPECIFIED }
            .map { chain ->
                when (BlockchainType.from(chain)) {
                    EVM_POS -> ethereumPosMultistream(chain, cachesFactory, headScheduler, tracer, indexConfig.getByChain(chain))
                    EVM_POW -> ethereumMultistream(chain, cachesFactory, headScheduler, tracer, indexConfig.getByChain(chain))
                    BITCOIN -> bitcoinMultistream(chain, cachesFactory, headScheduler)
                    STARKNET -> genericMultistream(chain, cachesFactory, headScheduler)
                }
            }
    }

    private fun genericMultistream(
        chain: Chain,
        cachesFactory: CachesFactory,
        headScheduler: Scheduler,
    ): Multistream {
        val name = "multi-$chain"
        return GenericMultistream(
            chain,
            CopyOnWriteArrayList(),
            cachesFactory.getCaches(chain),
            headScheduler,
        ).also { register(it, name) }
    }

    private fun ethereumMultistream(
        chain: Chain,
        cachesFactory: CachesFactory,
        headScheduler: Scheduler,
        tracer: Tracer,
        estimateLogsCountConfig: IndexConfig.Index? = null,
    ): EthereumMultistream {
        val name = "multi-ethereum-$chain"

        return EthereumMultistream(
            chain,
            CopyOnWriteArrayList(),
            cachesFactory.getCaches(chain),
            headScheduler,
            tracer,
            estimateLogsCountConfig,
        ).also { register(it, name) }
    }

    open fun ethereumPosMultistream(
        chain: Chain,
        cachesFactory: CachesFactory,
        headScheduler: Scheduler,
        tracer: Tracer,
        estimateLogsCountConfig: IndexConfig.Index? = null,
    ): EthereumPosMultiStream {
        val name = "multi-ethereum-pos-$chain"

        return EthereumPosMultiStream(
            chain,
            CopyOnWriteArrayList(),
            cachesFactory.getCaches(chain),
            headScheduler,
            tracer,
            estimateLogsCountConfig,
        ).also { register(it, name) }
    }

    open fun bitcoinMultistream(
        chain: Chain,
        cachesFactory: CachesFactory,
        headScheduler: Scheduler,
    ): BitcoinMultistream {
        val name = "multi-bitcoin-$chain"

        return BitcoinMultistream(
            chain,
            ArrayList(),
            cachesFactory.getCaches(chain),
            headScheduler,
        ).also { register(it, name) }
    }

    private fun register(bean: Any, name: String) {
        beanFactory.initializeBean(bean, name)
        beanFactory.autowireBean(bean)
        this.beanFactory.registerSingleton(name, bean)
    }
}
