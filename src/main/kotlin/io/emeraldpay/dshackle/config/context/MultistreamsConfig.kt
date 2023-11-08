package io.emeraldpay.dshackle.config.context

import io.emeraldpay.dshackle.BlockchainType.BITCOIN
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.cache.CachesFactory
import io.emeraldpay.dshackle.upstream.CallTargetsHolder
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.bitcoin.BitcoinMultistream
import io.emeraldpay.dshackle.upstream.generic.ChainSpecificRegistry
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
        multistreamEventsScheduler: Scheduler,
    ): List<Multistream> {
        return Chain.entries
            .filterNot { it == Chain.UNSPECIFIED }
            .map { chain ->
                if (chain.type == BITCOIN) {
                    bitcoinMultistream(chain, cachesFactory, headScheduler, multistreamEventsScheduler)
                } else {
                    genericMultistream(chain, cachesFactory, headScheduler, tracer, multistreamEventsScheduler)
                }
            }
    }

    private fun genericMultistream(
        chain: Chain,
        cachesFactory: CachesFactory,
        headScheduler: Scheduler,
        tracer: Tracer,
        multistreamEventsScheduler: Scheduler,
    ): Multistream {
        val name = "multi-$chain"
        val cs = ChainSpecificRegistry.resolve(chain)
        val caches = cachesFactory.getCaches(chain)
        return GenericMultistream(
            chain,
            multistreamEventsScheduler,
            cs.callSelector(caches),
            CopyOnWriteArrayList(),
            caches,
            headScheduler,
            cs.makeCachingReaderBuilder(tracer),
            cs::localReaderBuilder,
            cs.subscriptionBuilder(headScheduler),
        ).also { register(it, name) }
    }

    open fun bitcoinMultistream(
        chain: Chain,
        cachesFactory: CachesFactory,
        headScheduler: Scheduler,
        multistreamEventsScheduler: Scheduler,
    ): BitcoinMultistream {
        val name = "multi-bitcoin-$chain"

        return BitcoinMultistream(
            chain,
            multistreamEventsScheduler,
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
