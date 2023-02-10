package io.emeraldpay.dshackle.config

import org.yaml.snakeyaml.nodes.MappingNode
import java.io.InputStream

class CompressionConfigReader : YamlConfigReader(), ConfigReader<CompressionConfig> {
    fun read(input: InputStream): CompressionConfig? {
        val configNode = readNode(input)
        return read(configNode)
    }

    override fun read(input: MappingNode?): CompressionConfig {
        val config = CompressionConfig()
        getMapping(input, "compression")?.let { node ->
            readGRPC(config.grpc, getMapping(node, "grpc"))
        }
        return config
    }

    private fun readGRPC(grpcCompressionConfig: CompressionConfig.GRPC, input: MappingNode?) {
        getMapping(input, "server")?.let { serverNode ->
            getValueAsBool(serverNode, "enabled")?.let {
                grpcCompressionConfig.serverEnabled = it
            }
        }
        getMapping(input, "client")?.let { clientNode ->
            getValueAsBool(clientNode, "enabled")?.let {
                grpcCompressionConfig.clientEnabled = it
            }
        }
    }
}
