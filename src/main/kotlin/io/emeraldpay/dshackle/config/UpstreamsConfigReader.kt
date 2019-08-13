package io.emeraldpay.dshackle.config

import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.nodes.CollectionNode
import org.yaml.snakeyaml.nodes.MappingNode
import org.yaml.snakeyaml.nodes.Node
import org.yaml.snakeyaml.nodes.ScalarNode
import reactor.util.function.Tuples
import java.io.InputStream
import java.io.InputStreamReader
import java.lang.IllegalArgumentException
import java.net.URI

class UpstreamsConfigReader {

    private val log = LoggerFactory.getLogger(UpstreamsConfigReader::class.java)
    private val envRegex = Regex("\\$\\{(\\w+?)}")

    fun read(input: InputStream): UpstreamsConfig {
        val yaml = Yaml()
        val configNode = asMappingNode(yaml.compose(InputStreamReader(input)))

        val config = UpstreamsConfig()
        config.version = getValueAsString(configNode, "version")

        getList<MappingNode>(configNode, "defaultOptions")?.value?.forEach { opts ->
            val defaultOptions = UpstreamsConfig.DefaultOptions()
            config.defaultOptions.add(defaultOptions)
            defaultOptions.chains = getListOfString(opts, "chains")
            getMapping(opts, "options")?.let { values ->
                defaultOptions.options = readOptions(values)
            }
        }

        config.upstreams = ArrayList<UpstreamsConfig.Upstream<*>>()
        getList<MappingNode>(configNode, "upstreams")?.value?.forEach { upNode ->
            val connNode = getMapping(upNode, "connection")
            if (hasAny(connNode, "ethereum")) {
                val connConfigNode = getMapping(connNode, "ethereum")!!
                val upstream = UpstreamsConfig.Upstream<UpstreamsConfig.EthereumConnection>()
                readUpstreamCommon(upNode, upstream)
                readUpstreamEthereum(upNode, upstream)
                config.upstreams.add(upstream)
                val connection = UpstreamsConfig.EthereumConnection()
                upstream.connection = connection
                getMapping(connConfigNode, "rpc")?.let { node ->
                    getValueAsString(node, "url")?.let { url ->
                        val http = UpstreamsConfig.HttpEndpoint(URI(url))
                        connection.rpc = http
                        http.auth = readAuth(getMapping(node, "auth"))
                    }
                    readAuth(getMapping(node, "auth"))?.let { auth ->
                        connection.auth = auth as UpstreamsConfig.BasicAuth
                    }
                }
                getMapping(connConfigNode, "ws")?.let { node ->
                    getValueAsString(node, "url")?.let { url ->
                        val ws = UpstreamsConfig.WsEndpoint(URI(url))
                        connection.ws = ws
                        getValueAsString(node, "origin")?.let { origin ->
                            ws.origin = URI(origin)
                        }
//                    ws.auth = readAuth(getMapping(node, "auth"))
                    }
                }
            } else if (hasAny(connNode, "grpc")) {
                val connConfigNode = getMapping(connNode, "grpc")!!
                val upstream = UpstreamsConfig.Upstream<UpstreamsConfig.GrpcConnection>()
                readUpstreamCommon(upNode, upstream)
                readUpstreamGrpc(upNode, upstream)
                config.upstreams.add(upstream)
                val connection = UpstreamsConfig.GrpcConnection()
                upstream.connection = connection
                getValueAsString(connConfigNode, "host")?.let {
                    connection.host = it
                }
                getValueAsInt(connConfigNode, "port")?.let {
                    connection.port = it
                }
                connection.auth = readAuth(getMapping(connConfigNode, "auth")) as UpstreamsConfig.TlsAuth?
            }
        }

        return config
    }

    internal fun readUpstreamCommon(upNode: MappingNode, upstream: UpstreamsConfig.Upstream<*>) {
        upstream.id = getValueAsString(upNode, "id")
        upstream.options = tryReadOptions(upNode)
    }

    internal fun readUpstreamGrpc(upNode: MappingNode, upstream: UpstreamsConfig.Upstream<UpstreamsConfig.GrpcConnection>) {
        if (hasAny(upNode, "labels")) {
            log.warn("Labels are not applied to gRPC upstream")
        }
        if (hasAny(upNode, "chain")) {
            log.warn("Chain is not applied to gRPC upstream")
        }
    }

    internal fun readUpstreamEthereum(upNode: MappingNode, upstream: UpstreamsConfig.Upstream<UpstreamsConfig.EthereumConnection>) {
        upstream.chain = getValueAsString(upNode, "chain")
        if (hasAny(upNode, "labels")) {
            getMapping(upNode, "labels")?.let { labels ->
                labels.value.stream()
                        .filter { n -> n.keyNode is ScalarNode && n.valueNode is ScalarNode }
                        .map { n -> Tuples.of((n.keyNode as ScalarNode).value, (n.valueNode as ScalarNode).value)}
                        .map { kv -> Tuples.of(kv.t1.trim(), kv.t2.trim()) }
                        .filter { kv -> StringUtils.isNotEmpty(kv.t1) && StringUtils.isNotEmpty(kv.t2) }
                        .forEach { kv ->
                            upstream.labels[kv.t1] = kv.t2
                        }
            }
        }
    }

    internal fun tryReadOptions(upNode: MappingNode): UpstreamsConfig.Options? {
        return if (hasAny(upNode, "options")) {
            return getMapping(upNode, "options")?.let { values ->
                readOptions(values)
            }
        } else {
            null
        }
    }

    internal fun readOptions(values: MappingNode): UpstreamsConfig.Options {
        val options = UpstreamsConfig.Options()
        getValueAsInt(values, "min-peers")?.let {
            options.minPeers = it
        }
        getValueAsBool(values, "disable-validation")?.let {
            options.disableValidation = it
        }
        return options
    }

    private fun readAuth(authNode: MappingNode?): UpstreamsConfig.Auth? {
        return getValueAsString(authNode, "type")?.let {
            return when (it) {
                "tls" -> {
                    val auth = UpstreamsConfig.TlsAuth()
                    auth.ca = getValueAsString(authNode, "ca")
                    auth.certificate = getValueAsString(authNode, "certificate")
                    auth.key = getValueAsString(authNode, "key")
                    auth
                }
                "basic" -> {
                    val username = getValueAsString(authNode, "username")
                    val password = getValueAsString(authNode, "password")
                    if (username != null && password != null) {
                        UpstreamsConfig.BasicAuth(username, password)
                    } else {
                        log.warn("Basic auth is not fully configured")
                        null
                    }
                }
                else -> {
                    log.warn("Invalid Auth type: $it")
                    null
                }
            }
        }
    }

    private fun hasAny(mappingNode: MappingNode?, key: String): Boolean {
        if (mappingNode == null) {
            return false
        }
        return mappingNode.value
                .stream()
                .filter { n -> n.keyNode is ScalarNode }
                .filter { n ->
                    val sn = n.keyNode as ScalarNode
                    key == sn.value
                }.count() > 0
    }

    private fun <T> getValue(mappingNode: MappingNode?, key: String, type: Class<T>): T? {
        if (mappingNode == null) {
            return null
        }
        return mappingNode.value
                .stream()
                .filter { n -> n.keyNode is ScalarNode && type.isAssignableFrom(n.valueNode.javaClass) }
                .filter { n ->
                    val sn = n.keyNode as ScalarNode
                    key == sn.value
                }
                .map { n -> n.valueNode as T }
                .findFirst().let {
                    if (it.isPresent) {
                        it.get()
                    } else {
                        null
                    }
                }
    }

    private fun getMapping(mappingNode: MappingNode?, key: String): MappingNode? {
        return getValue(mappingNode, key, MappingNode::class.java)
    }

    private fun getValue(mappingNode: MappingNode?, key: String): ScalarNode? {
        return getValue(mappingNode, key, ScalarNode::class.java)
    }

    private fun <T> getList(mappingNode: MappingNode?, key: String): CollectionNode<T>? {
        return getValue(mappingNode, key, CollectionNode::class.java) as CollectionNode<T>
    }

    private fun getListOfString(mappingNode: MappingNode?, key: String): List<String>? {
        return getList<ScalarNode>(mappingNode, key)?.value
                ?.map { it.value }
                ?.map(this::postProcess)
    }

    private fun getValueAsString(mappingNode: MappingNode?, key: String): String? {
        return getValue(mappingNode, key)?.let {
             return@let it.value
        }?.let(this::postProcess)
    }

    private fun getValueAsInt(mappingNode: MappingNode?, key: String): Int? {
        return getValue(mappingNode, key)?.let {
            return@let if (it.isPlain) {
                it.value.toIntOrNull()
            } else {
                null
            }
        }
    }

    private fun getValueAsBool(mappingNode: MappingNode?, key: String): Boolean? {
        return getValue(mappingNode, key)?.let {
            return@let if (it.isPlain) {
                it.value?.toLowerCase() == "true"
            } else {
                null
            }
        }
    }

    private fun asMappingNode(node: Node): MappingNode {
        return if (MappingNode::class.java.isAssignableFrom(node.javaClass)) {
            node as MappingNode
        } else {
            throw IllegalArgumentException("Not a map")
        }
    }

    fun postProcess(value: String): String {
        return envRegex.replace(value) { m ->
            m.groups[1]?.let { g ->
                System.getProperty(g.value) ?: System.getenv(g.value) ?: ""
            } ?: ""
        }
    }
}