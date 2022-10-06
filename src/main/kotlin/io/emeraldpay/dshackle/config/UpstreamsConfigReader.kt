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
import org.yaml.snakeyaml.nodes.ScalarNode
import reactor.util.function.Tuples
import java.io.InputStream
import java.net.URI
import java.net.URISyntaxException
import java.util.Locale

class UpstreamsConfigReader(
    private val fileResolver: FileResolver
) : YamlConfigReader(), ConfigReader<UpstreamsConfig> {

    private val log = LoggerFactory.getLogger(UpstreamsConfigReader::class.java)
    private val authConfigReader = AuthConfigReader()

    fun read(input: InputStream): UpstreamsConfig? {
        val configNode = readNode(input)
        return readInternal(configNode)
    }

    override fun read(input: MappingNode?): UpstreamsConfig? {
        return getMapping(input, "cluster")?.let {
            readInternal(it)
        }
    }

    fun readInternal(input: MappingNode?): UpstreamsConfig? {
        val config = UpstreamsConfig()

        getList<MappingNode>(input, "defaults")?.value?.forEach { opts ->
            val defaultOptions = UpstreamsConfig.DefaultOptions()
            config.defaultOptions.add(defaultOptions)
            defaultOptions.blockchains = getListOfString(opts, "blockchains", "chains")
            if (hasAny(opts, "options")) {
                getMapping(opts, "options")
            } else {
                opts
            }?.let { values ->
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

        getList<MappingNode>(input, "upstreams")?.value?.forEachIndexed { _, upNode ->
            val connNode = getMapping(upNode, "connection")
            if (hasAny(connNode, "ethereum")) {
                val connConfigNode = getMapping(connNode, "ethereum")!!
                val upstream = UpstreamsConfig.Upstream<UpstreamsConfig.EthereumConnection>()
                readUpstreamCommon(upNode, upstream)
                readUpstreamStandard(upNode, upstream)
                if (isValid(upstream)) {
                    config.upstreams.add(upstream)
                    val connection = UpstreamsConfig.EthereumConnection()
                    upstream.connection = connection
                    getMapping(connConfigNode, "rpc")?.let { node ->
                        getValueAsString(node, "url")?.let { url ->
                            val http = UpstreamsConfig.HttpEndpoint(URI(url))
                            connection.rpc = http
                            http.basicAuth = authConfigReader.readClientBasicAuth(node)
                            http.tls = authConfigReader.readClientTls(node)
                        }
                    }
                    getMapping(connConfigNode, "ws")?.let { node ->
                        getValueAsString(node, "url")?.let { url ->
                            val ws = UpstreamsConfig.WsEndpoint(URI(url))
                            connection.ws = ws
                            getValueAsString(node, "origin")?.let { origin ->
                                ws.origin = URI(origin)
                            }
                            ws.basicAuth = authConfigReader.readClientBasicAuth(node)

                            getValueAsBytes(node, "frame-size", "frameSize")?.let {
                                if (it < 65_535) {
                                    throw IllegalStateException("frameSize cannot be less than 64Kb")
                                }
                                ws.frameSize = it
                            }
                            getValueAsBytes(node, "msg-size", "msgSize")?.let {
                                if (it < 65_535) {
                                    throw IllegalStateException("msgSize cannot be less than 64Kb")
                                }
                                ws.msgSize = it
                            }
                        }
                    }
                } else {
                    log.error("Upstream at #0 has invalid configuration")
                }
            } else if (hasAny(connNode, "bitcoin")) {
                val connConfigNode = getMapping(connNode, "bitcoin")!!
                val upstream = UpstreamsConfig.Upstream<UpstreamsConfig.BitcoinConnection>()
                readUpstreamCommon(upNode, upstream)
                readUpstreamStandard(upNode, upstream)
                if (isValid(upstream)) {
                    config.upstreams.add(upstream)
                    val connection = UpstreamsConfig.BitcoinConnection()
                    upstream.connection = connection
                    getMapping(connConfigNode, "rpc")?.let { node ->
                        getValueAsString(node, "url")?.let { url ->
                            val http = UpstreamsConfig.HttpEndpoint(URI(url))
                            connection.rpc = http
                            http.basicAuth = authConfigReader.readClientBasicAuth(node)
                            http.tls = authConfigReader.readClientTls(node)
                        }
                    }
                    getMapping(connConfigNode, "esplora")?.let { node ->
                        getValueAsString(node, "url")?.let { url ->
                            val http = UpstreamsConfig.HttpEndpoint(URI(url))
                            http.basicAuth = authConfigReader.readClientBasicAuth(node)
                            http.tls = authConfigReader.readClientTls(node)
                            connection.esplora = http
                        }
                    }
                    getMapping(connConfigNode, "zeromq")?.let { node ->
                        getValueAsString(node, "url", "address")?.let { address ->
                            // may be in form:
                            //   tcp://127.0.0.1:1234
                            //   127.0.0.1:1234
                            //   1234 <- assume localhost connection
                            val conn: Pair<String, Int> = parseHostPort(address, defaultHost = "127.0.0.1") {
                                "Invalid config for ZeroMQ: $address. Expected to be in format HOST:PORT"
                            }
                            connection.zeroMq = UpstreamsConfig.BitcoinZeroMq(conn.first, conn.second)
                        }
                    }
                } else {
                    log.error("Upstream at #0 has invalid configuration")
                }
            } else if (hasAny(connNode, "dshackle", "grpc")) {
                val connConfigNode = getMapping(connNode, "dshackle", "grpc")!!
                val upstream = UpstreamsConfig.Upstream<UpstreamsConfig.GrpcConnection>()
                readUpstreamCommon(upNode, upstream)
                readUpstreamGrpc(upNode, upstream)
                if (isValid(upstream)) {
                    config.upstreams.add(upstream)
                    val connection = UpstreamsConfig.GrpcConnection()
                    upstream.connection = connection
                    if (hasAny(connConfigNode, "url")) {
                        getValueAsString(connConfigNode, "url")?.let { address ->
                            try {
                                val uri = URI(address)
                                if (uri.host == null) {
                                    parseHostPort(address) {
                                        "Dshackle port is not specified in address: $address"
                                    }.let {
                                        connection.host = it.first
                                        connection.port = it.second
                                    }
                                } else {
                                    connection.host = uri.host
                                    connection.port = if (uri.port > 0) {
                                        uri.port
                                    } else if (uri.scheme == "https") {
                                        443
                                    } else if (uri.scheme == "http") {
                                        80
                                    } else {
                                        throw IllegalStateException("Dshackle port is not specified in address: $address")
                                    }
                                }

                                connection.autoTls = uri.scheme == "https"
                            } catch (t: Throwable) {
                                log.error("Invalid URL: $address. ${t.message}")
                            }
                        }
                    } else {
                        getValueAsString(connConfigNode, "host")?.let {
                            connection.host = it
                        }
                        getValueAsInt(connConfigNode, "port")?.let {
                            connection.port = it
                        }
                    }
                    connection.auth = authConfigReader.readClientTls(connConfigNode)
                } else {
                    log.error("Upstream at #0 has invalid configuration")
                }
            }
        }

        return config
    }

    fun parseHostPort(address: String, defaultHost: String? = null, defaultPort: Int? = null, error: (() -> String)?): Pair<String, Int> {
        return try {
            URI(address).let {
                // it tries to parse addresses like localhost:1234, but produces invalid URL
                if (it.port < 0 || it.host == null) {
                    throw URISyntaxException(address, "Invalid")
                }
                Pair(it.host, it.port)
            }
        } catch (t: URISyntaxException) {
            if (address.contains(":")) {
                address.split(":").let {
                    Pair(it[0], it[1].toInt())
                }
            } else if (defaultHost != null) {
                Pair(defaultHost, address.toInt())
            } else {
                throw IllegalArgumentException(error?.invoke() ?: "Invalid address: $address")
            }
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
        return true
    }

    internal fun readUpstreamCommon(upNode: MappingNode, upstream: UpstreamsConfig.Upstream<*>) {
        upstream.id = getValueAsString(upNode, "id")
        upstream.options = tryReadOptions(upNode)
        upstream.methods = tryReadMethods(upNode)
        getValueAsBool(upNode, "enabled")?.let {
            upstream.isEnabled = it
        }
    }

    internal fun readUpstreamGrpc(
        upNode: MappingNode,
        upstream: UpstreamsConfig.Upstream<UpstreamsConfig.GrpcConnection>
    ) {
        // Dshackle gRPC connection dispatches requests to different upstreams, which may
        // be on different blockchains, and each may have different set of labels.
        // So the labels and chains assigned to the gRPC connection make no sense.
        if (hasAny(upNode, "labels")) {
            // Actual labels from underlying upstreams are handled by GrpcUpstreamStatus
            log.warn("Labels should be not applied to gRPC upstream")
        }
        if (hasAny(upNode, "blockchain", "chain")) {
            log.warn("Chain should be not applied to gRPC upstream")
        }
    }

    internal fun readUpstreamStandard(upNode: MappingNode, upstream: UpstreamsConfig.Upstream<*>) {
        upstream.blockchain = getValueAsString(upNode, "blockchain", "chain")
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
        if (hasAny(upNode, "labels")) {
            getMapping(upNode, "labels")?.let { labels ->
                labels.value.stream()
                    .filter { n -> n.keyNode is ScalarNode && n.valueNode is ScalarNode }
                    .map { n -> Tuples.of((n.keyNode as ScalarNode).value, (n.valueNode as ScalarNode).value) }
                    .map { kv -> Tuples.of(kv.t1.trim(), kv.t2.trim()) }
                    .filter { kv -> StringUtils.isNotEmpty(kv.t1) && StringUtils.isNotEmpty(kv.t2) }
                    .forEach { kv ->
                        upstream.labels[kv.t1] = kv.t2
                    }
            }
        }
    }

    internal fun tryReadOptions(upNode: MappingNode): UpstreamsConfig.PartialOptions? {
        return if (hasAny(upNode, "options")) {
            return getMapping(upNode, "options")?.let { values ->
                readOptions(values)
            }
        } else {
            readOptions(upNode)
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
                enabled, disabled
            )
        }
    }

    internal fun readOptions(values: MappingNode): UpstreamsConfig.PartialOptions {
        val options = UpstreamsConfig.PartialOptions()
        getValueAsBool(values, "validate-peers")?.let {
            options.validatePeers = it
        }
        getValueAsBool(values, "validate-syncing")?.let {
            options.validateSyncing = it
        }
        getValueAsInt(values, "min-peers")?.let {
            options.minPeers = it
        }
        getValueAsInt(values, "timeout")?.let {
            options.timeout = it
        }
        getValueAsBool(values, "disable-validation")?.let {
            options.disableValidation = it
        }
        getValueAsInt(values, "validation-interval")?.let {
            options.validationInterval = it
        }
        getValueAsBool(values, "balance")?.let {
            options.providesBalance = it
        }
        getValueAsInt(values, "priority")?.let {
            options.priority = it
        }
        return options
    }
}
