package io.emeraldpay.dshackle.config

import io.emeraldpay.dshackle.foundation.ChainOptions
import io.emeraldpay.dshackle.foundation.ChainOptionsReader
import io.emeraldpay.dshackle.foundation.YamlConfigReader
import org.yaml.snakeyaml.DumperOptions
import org.yaml.snakeyaml.nodes.MappingNode
import org.yaml.snakeyaml.nodes.NodeTuple
import org.yaml.snakeyaml.nodes.ScalarNode
import org.yaml.snakeyaml.nodes.Tag
import java.math.BigInteger

class ChainsConfigReader(
    private val chainsOptionsReader: ChainOptionsReader,
) : YamlConfigReader<ChainsConfig>() {

    private val defaultConfig = this::class.java.getResourceAsStream("/chains.yaml")!!

    private fun readChains(input: MappingNode?): Map<Int, Pair<String, MappingNode>> {
        return getMapping(input, "chain-settings")?.let { config ->
            val default = getMapping(config, "default")
            getList<MappingNode>(config, "protocols")?.let { protocols ->
                protocols.value.fold(emptyMap()) { acc, protocol ->
                    val blockchain = getValueAsString(protocol, "id")
                        ?: throw IllegalArgumentException("Blockchain id is not defined")
                    val settings = mergeMappingNode(default, getMapping(protocol, "settings"))
                    acc.plus(
                        getList<MappingNode>(protocol, "chains")?.let { chains ->
                            chains.value.map { chain ->
                                val chainSettings = mergeMappingNode(settings, getMapping(chain, "settings"))
                                val updatedChain = mergeMappingNode(
                                    chain,
                                    MappingNode(
                                        chain.tag,
                                        listOf(
                                            NodeTuple(ScalarNode(Tag.STR, "settings", null, null, DumperOptions.ScalarStyle.LITERAL), chainSettings),
                                            NodeTuple(
                                                ScalarNode(Tag.STR, "blockchain", null, null, DumperOptions.ScalarStyle.LITERAL),
                                                ScalarNode(Tag.STR, blockchain, null, null, DumperOptions.ScalarStyle.LITERAL),
                                            ),
                                        ),
                                        chain.flowStyle,
                                    ),
                                )
                                val grpcId = getValueAsInt(updatedChain, "grpcId")
                                    ?: throw IllegalArgumentException("grpcId for chain is not defined")
                                Pair(grpcId, Pair(blockchain, updatedChain!!))
                            }
                        } ?: listOf(),
                    )
                }
            }
        } ?: emptyMap()
    }

    private fun parseChain(blockchain: String, node: MappingNode): ChainsConfig.ChainConfig {
        val id = getValueAsString(node, "id")
            ?: throw IllegalArgumentException("undefined id for $blockchain")
        val settings = getMapping(node, "settings") ?: throw IllegalArgumentException("undefined settings for $blockchain")
        val lags = getMapping(settings, "lags")?.let { lagConfig ->
            Pair(
                getValueAsInt(lagConfig, "syncing")
                    ?: throw IllegalArgumentException("undefined syncing for $blockchain"),
                getValueAsInt(lagConfig, "lagging")
                    ?: throw IllegalArgumentException("undefined lagging for $blockchain"),
            )
        } ?: throw IllegalArgumentException("undefined lags for $blockchain")
        val expectedBlockTime = getValueAsDuration(settings, "expected-block-time")
            ?: throw IllegalArgumentException("undefined expected block time")
        val validateContract = getValueAsString(node, "call-validate-contract")
        val options = chainsOptionsReader.read(settings) ?: ChainOptions.PartialOptions()
        val chainId = getValueAsString(node, "chain-id")
            ?: throw IllegalArgumentException("undefined chain id for $blockchain")
        val code = getValueAsString(node, "code")
            ?: throw IllegalArgumentException("undefined code for $blockchain")
        val grpcId = getValueAsInt(node, "grpcId")
            ?: throw IllegalArgumentException("undefined code for $blockchain")
        val netVersion = getValueAsLong(node, "net-version")?.toBigInteger() ?: BigInteger(chainId.drop(2), 16)
        val shortNames = getListOfString(node, "short-names")
            ?: throw IllegalArgumentException("undefined shortnames for $blockchain")
        return ChainsConfig.ChainConfig(
            expectedBlockTime = expectedBlockTime,
            syncingLagSize = lags.first,
            laggingLagSize = lags.second,
            options = options,
            callLimitContract = validateContract,
            chainId = chainId,
            code = code,
            grpcId = grpcId,
            netVersion = netVersion,
            shortNames = shortNames,
            id = id,
            blockchain = blockchain,
        )
    }

    override fun read(input: MappingNode?): ChainsConfig {
        val default = readChains(readNode(defaultConfig))
        val current = readChains(input)
        val chains = default.keys.toSet().plus(current.keys).map {
            val defChain = default[it]
            val curChain = current[it]
            when {
                curChain == null && defChain != null -> parseChain(defChain.first, defChain.second)
                curChain != null && defChain == null -> parseChain(curChain.first, curChain.second)
                curChain != null && defChain != null -> {
                    val merged = mergeMappingNode(defChain.second, curChain.second)
                    parseChain(defChain.first, merged!!)
                }
                else -> ChainsConfig.ChainConfig.default()
            }
        }
        return ChainsConfig(chains)
    }
}
