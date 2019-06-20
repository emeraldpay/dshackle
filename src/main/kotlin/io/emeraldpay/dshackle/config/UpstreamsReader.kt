package io.emeraldpay.dshackle.config

import org.yaml.snakeyaml.Yaml
import java.io.InputStream

class UpstreamsReader {

    fun read(input: InputStream): Upstreams {
        val yaml = Yaml()
        yaml.addTypeDescription(Upstreams.EndpointTypeYaml())
        yaml.addTypeDescription(Upstreams.OptionsYaml())
        yaml.addTypeDescription(Upstreams.AuthYaml())
        return yaml.loadAs(input, Upstreams::class.java)
    }


}