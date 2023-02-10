package io.emeraldpay.dshackle.config

import io.emeraldpay.dshackle.Global
import org.yaml.snakeyaml.nodes.CollectionNode
import org.yaml.snakeyaml.nodes.MappingNode

class ChainsConfigReader : YamlConfigReader<ChainsConfig>() {

    override fun read(input: MappingNode?): ChainsConfig {
        return getMapping(input, "chain-settings")?.let {

            val chains = getList<MappingNode>(it, "chains")?.let {
                readChains(it)
            }

            val default = getMapping(it, "default")?.let {
                readChain(it)
            }

            return ChainsConfig(
                chains
                    ?.map { Global.chainById(it.first) to it.second }
                    ?.associateBy({ it.first }, { it.second }) ?: emptyMap(),
                default
            )
        } ?: ChainsConfig.default()
    }

    private fun readChain(node: MappingNode): ChainsConfig.RawChainConfig? {
        return getMapping(node, "lags")?.let {
            return ChainsConfig.RawChainConfig(
                getValueAsInt(it, "syncing"),
                getValueAsInt(it, "lagging")
            )
        }
    }

    private fun readChains(node: CollectionNode<MappingNode>): List<Pair<String, ChainsConfig.RawChainConfig>> {
        return node.value.mapNotNull {
            val key = getValueAsString(it, "name")
                ?: throw InvalidConfigYamlException(filename, it.startMark, "chain name required")
            val value = readChain(it)
            if (value != null) {
                return@mapNotNull key to value
            } else {
                return@mapNotNull null
            }
        }
    }
}
