/**
 * Copyright (c) 2020 EmeraldPay, Inc
 * Copyright (c) 2019 ETCDEV GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.emeraldpay.dshackle.config

import io.emeraldpay.dshackle.FileResolver
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.yaml.snakeyaml.nodes.MappingNode
import java.net.URI
import java.time.Duration
import java.util.Locale

class UpstreamsConfigReader(
    private val fileResolver: FileResolver
) : YamlConfigReader<UpstreamsConfig>() {

    private val log = LoggerFactory.getLogger(UpstreamsConfigReader::class.java)
    private val authConfigReader = AuthConfigReader()
    private val knownNodeIds: MutableSet<Int> = HashSet()

    override fun read(input: MappingNode?): UpstreamsConfig? {
        return getMapping(input, "cluster")?.let {
            readInternal(it)
        }
    }

    fun readInternal(input: MappingNode?): UpstreamsConfig {
        val config = UpstreamsConfig()

        getList<MappingNode>(input, "defaults")?.value?.forEach { opts ->
            val defaultOptions = UpstreamsConfig.DefaultOptions()
            config.defaultOptions.add(defaultOptions)
            defaultOptions.chains = getListOfString(opts, "chains")
            getMapping(opts, "options")?.let { values ->
                defaultOptions.options = readOptions(values)
            }
        }

        config.upstreams = ArrayList<UpstreamsConfig.Upstream<*>>()

        getValueAsString(input, "include")?.let { path ->
            fileResolver.resolve(path).let { file ->
                if (file.exists() && file.isFile && file.canRead()) {
                    read(file.inputStream())?.let {
                        it.upstreams.forEach { upstream -> config.upstreams.add(upstream) }
                    }
                } else {
                    log.warn("Failed to read config from $path")
                }
            }
        }

        getListOfString(input, "include")?.forEach { path ->
            fileResolver.resolve(path).let { file ->
                if (file.exists() && file.isFile && file.canRead()) {
                    read(file.inputStream())?.let {
                        it.upstreams.forEach { upstream -> config.upstreams.add(upstream) }
                    }
                } else {
                    log.warn("Failed to read config from $path")
                }
            }
        }

        getList<MappingNode>(input, "upstreams")?.value?.forEach { upNode ->
            val connNode = getMapping(upNode, "connection")
            if (hasAny(connNode, "ethereum")) {
                readUpstream(config, upNode) {
                    readEthereumConnection(getMapping(connNode, "ethereum")!!)
                }
            } else if (hasAny(connNode, "bitcoin")) {
                readUpstream(config, upNode) {
                    readBitcoinConnection(getMapping(connNode, "bitcoin")!!)
                }
            } else if (hasAny(connNode, "ethereum-pos")) {
                readUpstream(config, upNode) {
                    readEthereumPosConnection(getMapping(connNode, "ethereum-pos")!!)
                }
            } else if (hasAny(connNode, "grpc")) {
                val connConfigNode = getMapping(connNode, "grpc")!!
                val upstream = UpstreamsConfig.Upstream<UpstreamsConfig.GrpcConnection>()
                readUpstreamCommon(upNode, upstream)
                readUpstreamGrpc(upNode)
                if (isValid(upstream)) {
                    config.upstreams.add(upstream)
                    val connection = UpstreamsConfig.GrpcConnection()
                    upstream.connection = connection
                    getValueAsInt(connConfigNode, "upstream-rating")?.let {
                        connection.upstreamRating = it
                    }
                    getValueAsString(connConfigNode, "host")?.let {
                        connection.host = it
                    }
                    getValueAsInt(connConfigNode, "port")?.let {
                        connection.port = it
                    }
                    connection.auth = authConfigReader.readClientTls(connConfigNode)
                } else {
                    log.error("Upstream at #0 has invalid configuration")
                }
            }
        }

        return config
    }

    private fun readBitcoinConnection(connConfigNode: MappingNode): UpstreamsConfig.BitcoinConnection {
        val connection = UpstreamsConfig.BitcoinConnection()
            .apply { rpc = readRpcConfig(connConfigNode) }

        getMapping(connConfigNode, "esplora")?.let { node ->
            getValueAsString(node, "url")?.let { url ->
                val http = UpstreamsConfig.HttpEndpoint(URI(url))
                http.basicAuth = authConfigReader.readClientBasicAuth(node)
                http.tls = authConfigReader.readClientTls(node)
                connection.esplora = http
            }
        }
        getMapping(connConfigNode, "zeromq")?.let { node ->
            getValueAsString(node, "address")?.let { address ->
                val zmqConfig: Pair<String, Int>? = try {
                    if (address.contains(":")) {
                        address.split(":").let {
                            Pair(it[0], it[1].toInt())
                        }
                    } else {
                        Pair("127.0.0.1", address.toInt())
                    }
                } catch (t: Throwable) {
                    log.warn("Invalid config for ZeroMQ: $address. Expected to be in format HOST:PORT")
                    null
                }
                zmqConfig?.let {
                    connection.zeroMq = UpstreamsConfig.BitcoinZeroMq(it.first, it.second)
                }
            }
        }
        return connection
    }

    private fun readRpcConfig(connConfigNode: MappingNode): UpstreamsConfig.HttpEndpoint? {
        return getMapping(connConfigNode, "rpc")?.let { node ->
            getValueAsString(node, "url")?.let { url ->
                val http = UpstreamsConfig.HttpEndpoint(URI(url))
                http.basicAuth = authConfigReader.readClientBasicAuth(node)
                http.tls = authConfigReader.readClientTls(node)
                http
            }
        }
    }

    private fun readEthereumPosConnection(connConfigNode: MappingNode): UpstreamsConfig.EthereumPosConnection {
        val connection = UpstreamsConfig.EthereumPosConnection()
        getMapping(connConfigNode, "execution")?.let {
            connection.execution = readEthereumConnection(it)
        }
        getValueAsInt(connConfigNode, "upstream-rating")?.let {
            connection.upstreamRating = it
        }
        return connection
    }

    private fun readEthereumConnection(connConfigNode: MappingNode): UpstreamsConfig.EthereumConnection {
        val connection = UpstreamsConfig.EthereumConnection()
            .apply { rpc = readRpcConfig(connConfigNode) }

        getValueAsBool(connConfigNode, "prefer-http")?.let {
            connection.preferHttp = it
        }
        getMapping(connConfigNode, "ws")?.let { node ->
            getValueAsString(node, "url")?.let { url ->
                val ws = UpstreamsConfig.WsEndpoint(URI(url))
                connection.ws = ws
                getValueAsString(node, "origin")?.let { origin ->
                    ws.origin = URI(origin)
                }
                ws.basicAuth = authConfigReader.readClientBasicAuth(node)

                getValueAsBytes(node, "frameSize")?.let {
                    if (it < 65_535) {
                        throw IllegalStateException("frameSize cannot be less than 64Kb")
                    }
                    ws.frameSize = it
                }
                getValueAsBytes(node, "msgSize")?.let {
                    if (it < 65_535) {
                        throw IllegalStateException("msgSize cannot be less than 64Kb")
                    }
                    ws.msgSize = it
                }
                getValueAsInt(node, "connections")?.let {
                    if (it < 1 || it > 1024) {
                        throw IllegalStateException("connection limit should be in 1..1024")
                    }
                    ws.connections = it
                }
            }
        }
        return connection
    }

    private fun <T : UpstreamsConfig.UpstreamConnection> readUpstream(
        config: UpstreamsConfig,
        upNode: MappingNode,
        connFactory: () -> T
    ) {
        val upstream = UpstreamsConfig.Upstream<T>()
        readUpstreamCommon(upNode, upstream)
        readUpstreamStandard(upNode, upstream)
        if (isValid(upstream)) {
            config.upstreams.add(upstream)
            upstream.connection = connFactory()
        } else {
            log.error("Upstream at #0 has invalid configuration")
        }
    }

    fun isValid(upstream: UpstreamsConfig.Upstream<*>): Boolean {
        val id = upstream.id
        // In general, we just check that id is suitable for urls and references,
        // Besides that, if Response Signatures are enabled (CurrentResponseSigner) then it's critical that the id cannot have `/` symbol
        if (id == null || id.length < 3 || !id.matches(Regex("[a-zA-Z][a-zA-Z0-9_-]+[a-zA-Z0-9]"))) {
            log.warn("Invalid id: $id")
            return false
        }
        return upstream.nodeId?.let {
            if (it !in 1..255) {
                log.warn("Invalid node-id: $it. Must be in range [1, 255].")
                false
            } else if (!knownNodeIds.add(it)) {
                log.warn("Duplicated node-id: $it. Must be in unique.")
                false
            } else {
                true
            }
        } ?: true
    }

    internal fun readUpstreamCommon(upNode: MappingNode, upstream: UpstreamsConfig.Upstream<*>) {
        upstream.id = getValueAsString(upNode, "id")
        upstream.nodeId = getValueAsInt(upNode, "node-id")
        upstream.options = tryReadOptions(upNode)
        upstream.methods = tryReadMethods(upNode)
        upstream.methodGroups = tryReadMethodGroups(upNode)
        getValueAsBool(upNode, "enabled")?.let {
            upstream.isEnabled = it
        }
        if (hasAny(upNode, "labels")) {
            getMapping(upNode, "labels")?.let { labels ->
                labels.value.stream()
                    .map { it.keyNode.valueAsString() to it.valueNode.valueAsString() }
                    .filter { StringUtils.isNotBlank(it.first) && StringUtils.isNotBlank(it.second) }
                    .forEach {
                        upstream.labels[it.first!!.trim()] = it.second!!.trim()
                    }
            }
        }
    }

    internal fun readUpstreamGrpc(
        upNode: MappingNode
    ) {
        if (hasAny(upNode, "chain")) {
            log.warn("Chain should be not applied to gRPC upstream")
        }
    }

    internal fun readUpstreamStandard(upNode: MappingNode, upstream: UpstreamsConfig.Upstream<*>) {
        upstream.chain = getValueAsString(upNode, "chain")
        getValueAsString(upNode, "role")?.let {
            val name = it.trim().let {
                // `standard` was initial role, now split into `primary` and `secondary`
                if (it == "standard") "primary" else it
            }
            try {
                val role = UpstreamsConfig.UpstreamRole.valueOf(name.uppercase(Locale.getDefault()))
                upstream.role = role
            } catch (e: IllegalArgumentException) {
                log.warn("Unsupported role `$name` for upstream ${upstream.id}")
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

    internal fun tryReadMethods(upNode: MappingNode): UpstreamsConfig.Methods? {
        return getMapping(upNode, "methods")?.let { mnode ->
            val enabled = getList<MappingNode>(mnode, "enabled")?.value?.map { m ->
                getValueAsString(m, "name")?.let { name ->
                    UpstreamsConfig.Method(
                        name = name,
                        quorum = getValueAsString(m, "quorum"),
                        static = getValueAsString(m, "static")
                    )
                }
            }?.filterNotNull()?.toSet() ?: emptySet()
            val disabled = getList<MappingNode>(mnode, "disabled")?.value?.map { m ->
                getValueAsString(m, "name")?.let { name ->
                    UpstreamsConfig.Method(
                        name = name
                    )
                }
            }?.filterNotNull()?.toSet() ?: emptySet()

            UpstreamsConfig.Methods(
                enabled,
                disabled
            )
        }
    }

    internal fun tryReadMethodGroups(upNode: MappingNode): UpstreamsConfig.MethodGroups? {
        return getMapping(upNode, "method-groups")?.let {
            UpstreamsConfig.MethodGroups(
                enabled = getListOfString(it, "enabled")?.toSet() ?: emptySet(),
                disabled = getListOfString(it, "disabled")?.toSet() ?: emptySet()
            )
        }
    }

    internal fun readOptions(values: MappingNode): UpstreamsConfig.Options {
        val options = UpstreamsConfig.Options()
        getValueAsInt(values, "min-peers")?.let {
            options.minPeers = it
        }
        getValueAsInt(values, "timeout")?.let {
            options.timeout = Duration.ofSeconds(it.toLong())
        }
        getValueAsBool(values, "disable-validation")?.let {
            options.disableValidation = it
        }
        getValueAsBool(values, "balance")?.let {
            options.providesBalance = it
        }
        return options
    }
}
