package io.emeraldpay.dshackle.config.reload

import io.emeraldpay.dshackle.Config
import io.emeraldpay.dshackle.FileResolver
import io.emeraldpay.dshackle.config.MainConfig
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.config.UpstreamsConfigReader
import io.emeraldpay.dshackle.foundation.ChainOptionsReader
import org.springframework.stereotype.Component

@Component
class ReloadConfigService(
    private val config: Config,
    fileResolver: FileResolver,
    private val mainConfig: MainConfig,

) {
    private val optionsReader = ChainOptionsReader()
    private val upstreamsConfigReader = UpstreamsConfigReader(fileResolver, optionsReader)

    fun readUpstreamsConfig() = upstreamsConfigReader.read(config.getConfigPath().inputStream())!!

    fun currentUpstreamsConfig() = mainConfig.upstreams!!

    fun updateUpstreamsConfig(newConfig: UpstreamsConfig) {
        mainConfig.upstreams = newConfig
    }
}
