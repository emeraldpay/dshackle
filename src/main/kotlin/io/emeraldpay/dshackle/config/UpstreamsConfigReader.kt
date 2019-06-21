package io.emeraldpay.dshackle.config

import org.yaml.snakeyaml.Yaml
import java.io.InputStream

class UpstreamsConfigReader {

    fun read(input: InputStream): UpstreamsConfig {
        val yaml = Yaml()
        yaml.addTypeDescription(UpstreamsConfig.EndpointTypeYaml())
        yaml.addTypeDescription(UpstreamsConfig.OptionsYaml())
        yaml.addTypeDescription(UpstreamsConfig.AuthYaml())
        return yaml.loadAs(input, UpstreamsConfig::class.java)
    }


}