package io.emeraldpay.dshackle.config

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.foundation.YamlConfigReader
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.springframework.util.unit.DataSize
import org.yaml.snakeyaml.nodes.MappingNode

class IndexConfigReader : YamlConfigReader<IndexConfig>() {

    companion object {
        private val log = LoggerFactory.getLogger(IndexConfigReader::class.java)
    }

    override fun read(input: MappingNode?): IndexConfig? {
        return getList<MappingNode>(input, "index")?.let { items ->
            val config = IndexConfig()

            items.value.map {
                val blockchainRaw = getValueAsString(it, "chain")
                if (blockchainRaw == null || StringUtils.isEmpty(blockchainRaw) || Global.chainById(blockchainRaw) == Chain.UNSPECIFIED) {
                    throw InvalidConfigYamlException(filename, it.startMark, "Invalid blockchain or not specified")
                }

                val blockchain = Global.chainById(blockchainRaw)
                if (config.items.containsKey(blockchain)) {
                    throw InvalidConfigYamlException(filename, it.startMark, "Duplicated indexes")
                }

                val rpc = getValueAsString(it, "rpc")
                if (rpc == null || StringUtils.isEmpty(rpc)) {
                    throw InvalidConfigYamlException(filename, it.startMark, "Invalid rpc specified")
                }

                val store = getValueAsString(it, "store")
                if (store == null || StringUtils.isEmpty(store)) {
                    throw InvalidConfigYamlException(filename, it.startMark, "Invalid store directory or not specified")
                }

                val limit = getMapping(it, "limit")
                val ram_limit = limit?.let {
                    val raw = getValueAsString(limit, "ram")
                    if (raw == null || StringUtils.isEmpty(raw)) {
                        return null
                    }

                    try {
                        DataSize.parse(raw).toBytes()
                    } catch (e: IllegalArgumentException) {
                        throw InvalidConfigYamlException(filename, it.startMark, "Invalid limit for index")
                    }
                }

                config.items.put(blockchain, IndexConfig.Index(rpc, store, ram_limit))
            }

            config
        }
    }
}
