package io.emeraldpay.dshackle.config

import io.emeraldpay.dshackle.Global
import org.yaml.snakeyaml.nodes.CollectionNode
import org.yaml.snakeyaml.nodes.MappingNode
import java.io.InputStream

class ChainsConfigReader(
    private val upstreamsConfigReader: UpstreamsConfigReader
) : YamlConfigReader<ChainsConfig>() {

    private val defaultConfig = this::class.java.getResourceAsStream("/chains.yaml")!!

    override fun read(input: MappingNode?): ChainsConfig {
        val default = readInternal(defaultConfig)
        val current = readInternal(input)
        return default.patch(current)
    }

    fun readInternal(input: InputStream): ChainsConfig {
        val configNode = readNode(input)
        return readInternal(configNode)
    }

    fun readInternal(input: MappingNode?): ChainsConfig {
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
        val rawConfig = ChainsConfig.RawChainConfig()
        getMapping(node, "lags")?.let { lagConfig ->
            getValueAsInt(lagConfig, "syncing")?.let {
                rawConfig.syncingLagSize = it
            }
            getValueAsInt(lagConfig, "lagging")?.let {
                rawConfig.laggingLagSize = it
            }
        }
        upstreamsConfigReader.tryReadOptions(node)?.let {
            rawConfig.options = it
        }

        return rawConfig
    }

    private fun readChains(node: CollectionNode<MappingNode>): List<Pair<String, ChainsConfig.RawChainConfig>> {
        return node.value.mapNotNull {
            val key = getValueAsString(it, "id")
                ?: throw InvalidConfigYamlException(filename, it.startMark, "chain id required")
            val value = readChain(it)
            if (value != null) {
                return@mapNotNull key to value
            } else {
                return@mapNotNull null
            }
        }
    }
}
