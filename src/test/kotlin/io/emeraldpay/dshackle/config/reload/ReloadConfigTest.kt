package io.emeraldpay.dshackle.config.reload

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Chain.ETHEREUM__MAINNET
import io.emeraldpay.dshackle.Chain.POLYGON__MAINNET
import io.emeraldpay.dshackle.Config
import io.emeraldpay.dshackle.FileResolver
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.config.MainConfig
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.config.UpstreamsConfigReader
import io.emeraldpay.dshackle.foundation.ChainOptionsReader
import io.emeraldpay.dshackle.startup.ConfiguredUpstreams
import io.emeraldpay.dshackle.startup.UpstreamChangeEvent
import io.emeraldpay.dshackle.upstream.CurrentMultistreamHolder
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.generic.ChainSpecificRegistry
import io.emeraldpay.dshackle.upstream.generic.GenericMultistream
import io.emeraldpay.dshackle.upstream.generic.GenericUpstream
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.springframework.cloud.sleuth.Tracer
import org.springframework.util.ResourceUtils
import reactor.core.publisher.Flux
import reactor.core.scheduler.Schedulers
import sun.misc.Signal
import java.io.File
import java.util.concurrent.Executors

class ReloadConfigTest {
    private val fileResolver = FileResolver(File(""))
    private val mainConfig = MainConfig()

    private val optionsReader = ChainOptionsReader()
    private val upstreamsConfigReader = UpstreamsConfigReader(fileResolver, optionsReader)

    private val config = mock<Config>()
    private val reloadConfigService = ReloadConfigService(config, fileResolver, mainConfig)
    private val configuredUpstreams = mock<ConfiguredUpstreams>()

    @BeforeEach
    fun setupTests() {
        mainConfig.upstreams = null
    }

    @Test
    fun `reload upstreams changes`() {
        val up1 = upstream("local1")
        val up2 = upstream("local2")
        val up3 = upstream("local3")

        val msEth = mock<Multistream> {
            on { getAll() } doReturn listOf(up1, up2)
        }
        val msPoly = mock<Multistream> {
            on { getAll() } doReturn listOf(up3)
        }
        val newConfigFile = ResourceUtils.getFile("classpath:configs/upstreams-changed.yaml")
        whenever(config.getConfigPath()).thenReturn(newConfigFile)

        val currentMultistreamHolder = mock<CurrentMultistreamHolder> {
            on { getUpstream(ETHEREUM__MAINNET) } doReturn msEth
            on { getUpstream(POLYGON__MAINNET) } doReturn msPoly
        }
        val reloadConfigUpstreamService = ReloadConfigUpstreamService(
            currentMultistreamHolder,
            configuredUpstreams,
        )
        val reloadConfig = ReloadConfigSetup(reloadConfigService, reloadConfigUpstreamService)

        val initialConfigIs = ResourceUtils.getFile("classpath:configs/upstreams-initial.yaml").inputStream()
        val initialConfig = upstreamsConfigReader.read(initialConfigIs)!!
        val newConfig = upstreamsConfigReader.read(newConfigFile.inputStream())!!
        mainConfig.upstreams = initialConfig

        reloadConfig.handle(Signal("HUP"))

        verify(msEth).processUpstreamsEvents(UpstreamChangeEvent(ETHEREUM__MAINNET, up1, UpstreamChangeEvent.ChangeType.REMOVED))
        verify(msPoly).processUpstreamsEvents(UpstreamChangeEvent(POLYGON__MAINNET, up3, UpstreamChangeEvent.ChangeType.REMOVED))
        verify(configuredUpstreams).processUpstreams(
            UpstreamsConfig(
                newConfig.defaultOptions,
                mutableListOf(newConfig.upstreams[0], newConfig.upstreams[2]),
            ),
        )
        verify(msEth, never()).stop()
        verify(msPoly, never()).stop()

        assertEquals(3, mainConfig.upstreams!!.upstreams.size)
        assertEquals(newConfig, mainConfig.upstreams)
    }

    @Test
    fun `stop multistream if all upstreams are removed from it`() {
        val up1 = upstream("local1")
        val up2 = upstream("local2")
        val up3 = upstream("local3")

        val msEth = multistream(ETHEREUM__MAINNET)
            .also {
                it.processUpstreamsEvents(UpstreamChangeEvent(ETHEREUM__MAINNET, up1, UpstreamChangeEvent.ChangeType.ADDED))
                it.processUpstreamsEvents(UpstreamChangeEvent(ETHEREUM__MAINNET, up2, UpstreamChangeEvent.ChangeType.ADDED))
            }
        val msPoly = multistream(POLYGON__MAINNET)
            .also {
                it.processUpstreamsEvents(UpstreamChangeEvent(POLYGON__MAINNET, up3, UpstreamChangeEvent.ChangeType.ADDED))
            }

        val newConfigFile = ResourceUtils.getFile("classpath:configs/upstreams-changed-upstreams-removed.yaml")
        whenever(config.getConfigPath()).thenReturn(newConfigFile)

        val currentMultistreamHolder = mock<CurrentMultistreamHolder> {
            on { getUpstream(ETHEREUM__MAINNET) } doReturn msEth
            on { getUpstream(POLYGON__MAINNET) } doReturn msPoly
        }
        val reloadConfigUpstreamService = ReloadConfigUpstreamService(
            currentMultistreamHolder,
            configuredUpstreams,
        )
        val reloadConfig = ReloadConfigSetup(reloadConfigService, reloadConfigUpstreamService)
        val initialConfigIs = ResourceUtils.getFile("classpath:configs/upstreams-initial.yaml").inputStream()
        val initialConfig = upstreamsConfigReader.read(initialConfigIs)!!
        val newConfig = upstreamsConfigReader.read(newConfigFile.inputStream())!!
        mainConfig.upstreams = initialConfig

        reloadConfig.handle(Signal("HUP"))

        verify(configuredUpstreams).processUpstreams(
            UpstreamsConfig(
                newConfig.defaultOptions,
                mutableListOf(),
            ),
        )

        assertFalse(msEth.isRunning())
        assertTrue(msPoly.isRunning())
        assertEquals(1, mainConfig.upstreams!!.upstreams.size)
        assertEquals(newConfig, mainConfig.upstreams)
    }

    @Test
    fun `reload the same config cause to nothing`() {
        val initialConfigFile = ResourceUtils.getFile("classpath:configs/upstreams-initial.yaml")
        val initialConfig = upstreamsConfigReader.read(initialConfigFile.inputStream())!!
        mainConfig.upstreams = initialConfig

        val reloadConfigUpstreamService = mock<ReloadConfigUpstreamService>()

        val reloadConfig = ReloadConfigSetup(reloadConfigService, reloadConfigUpstreamService)

        whenever(config.getConfigPath()).thenReturn(initialConfigFile)

        reloadConfig.handle(Signal("HUP"))

        verify(reloadConfigUpstreamService, never()).reloadUpstreams(any(), any(), any(), any())

        assertEquals(initialConfig, mainConfig.upstreams)
    }

    private fun multistream(chain: Chain): Multistream {
        val cs = ChainSpecificRegistry.resolve(chain)
        return GenericMultistream(
            chain,
            Schedulers.fromExecutor(Executors.newFixedThreadPool(6)),
            null,
            ArrayList(),
            Caches.default(),
            Schedulers.boundedElastic(),
            cs.makeCachingReaderBuilder(mock<Tracer>()),
            cs::localReaderBuilder,
            cs.subscriptionBuilder(Schedulers.boundedElastic()),
            null,
            Schedulers.fromExecutor(Executors.newFixedThreadPool(6)),
        )
    }

    private fun upstream(id: String): GenericUpstream =
        mock {
            val head = mock<Head> {
                on { getFlux() } doReturn Flux.empty()
            }
            on { getId() } doReturn id
            on { getHead() } doReturn head
            on { observeState() } doReturn Flux.empty()
            on { observeStatus() } doReturn Flux.empty()
        }
}
