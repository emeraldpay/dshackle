package io.emeraldpay.dshackle.config

import io.emeraldpay.dshackle.Global
import org.yaml.snakeyaml.nodes.CollectionNode
import org.yaml.snakeyaml.nodes.MappingNode
import java.io.InputStream

class ChainsConfigReader : YamlConfigReader(), ConfigReader<ChainsConfig> {

    fun read(input: InputStream): ChainsConfig {
        val configNode = readNode(input)
        return read(configNode)
    }

    override fun read(input: MappingNode?): ChainsConfig {
        val chains = getList<MappingNode>(input, "chains")?.let {
            readChains(it)
        }

        if (chains == null) {
            return ChainsConfig.default()
        } else {

            val default = chains.firstOrNull { it.first == "default" }?.second ?: ChainsConfig.ChainConfig.default()

            return ChainsConfig(
                chains.filter { it.first != "default" }
                    .map { Global.chainById(it.first) to it.second }
                    .associateBy({ it.first }, { it.second }),
                default
            )
        }
    }

    private fun readChains(node: CollectionNode<MappingNode>): List<Pair<String, ChainsConfig.ChainConfig>> {
        return node.value.map {
            val key = getValueAsString(it, "name") ?: throw IllegalArgumentException()
            val value = ChainsConfig.ChainConfig(
                getValueAsInt(it, "syncing-size") ?: throw IllegalArgumentException(),
                getValueAsInt(it, "lagging-size") ?: throw IllegalArgumentException()
            )
            key to value
        }
    }
}
