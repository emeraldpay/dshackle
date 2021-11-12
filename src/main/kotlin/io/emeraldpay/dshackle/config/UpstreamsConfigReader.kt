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
import java.time.Duration
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
                } else {
                    log.error("Upstream at #0 has invalid configuration")
                }
            } else if (hasAny(connNode, "grpc")) {
                val connConfigNode = getMapping(connNode, "grpc")!!
                val upstream = UpstreamsConfig.Upstream<UpstreamsConfig.GrpcConnection>()
                readUpstreamCommon(upNode, upstream)
                readUpstreamGrpc(upNode, upstream)
                if (isValid(upstream)) {
                    config.upstreams.add(upstream)
                    val connection = UpstreamsConfig.GrpcConnection()
                    upstream.connection = connection
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

    fun isValid(upstream: UpstreamsConfig.Upstream<*>): Boolean {
        val id = upstream.id
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
    }

    internal fun readUpstreamGrpc(
        upNode: MappingNode,
        upstream: UpstreamsConfig.Upstream<UpstreamsConfig.GrpcConnection>
    ) {
        if (hasAny(upNode, "labels")) {
            log.warn("Labels should be not applied to gRPC upstream")
        }
        if (hasAny(upNode, "chain")) {
            log.warn("Chain should be not applied to gRPC upstream")
        }
    }

    internal fun readUpstreamStandard(upNode: MappingNode, upstream: UpstreamsConfig.Upstream<*>) {
        upstream.chain = getValueAsString(upNode, "chain")
        getValueAsString(upNode, "role")?.let {
            val name = it.trim()
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
                enabled, disabled
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
