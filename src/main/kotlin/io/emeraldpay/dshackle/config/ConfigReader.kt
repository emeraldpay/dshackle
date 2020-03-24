package io.emeraldpay.dshackle.config

import org.yaml.snakeyaml.nodes.MappingNode

interface ConfigReader<T> {

    fun read(input: MappingNode?): T?

}