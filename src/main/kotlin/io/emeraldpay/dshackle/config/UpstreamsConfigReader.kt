/**
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
import java.time.Duration

class UpstreamsConfigReader : YamlConfigReader() {

    private val log = LoggerFactory.getLogger(UpstreamsConfigReader::class.java)

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
        getList<MappingNode>(configNode, "upstreams")?.value?.forEachIndexed { pos, upNode ->
            val connNode = getMapping(upNode, "connection")
            if (hasAny(connNode, "ethereum")) {
                val connConfigNode = getMapping(connNode, "ethereum")!!
                val upstream = UpstreamsConfig.Upstream<UpstreamsConfig.EthereumConnection>()
                readUpstreamCommon(upNode, upstream)
                readUpstreamEthereum(upNode, upstream)
                if (isValid(upstream)) {
                    config.upstreams.add(upstream)
                    val connection = UpstreamsConfig.EthereumConnection()
                    upstream.connection = connection
                    getMapping(connConfigNode, "rpc")?.let { node ->
                        getValueAsString(node, "url")?.let { url ->
                            val http = UpstreamsConfig.HttpEndpoint(URI(url))
                            connection.rpc = http
                            http.basicAuth = readBasicAuth(node)
                            http.tls = readTls(node)
                        }
                    }
                    getMapping(connConfigNode, "ws")?.let { node ->
                        getValueAsString(node, "url")?.let { url ->
                            val ws = UpstreamsConfig.WsEndpoint(URI(url))
                            connection.ws = ws
                            getValueAsString(node, "origin")?.let { origin ->
                                ws.origin = URI(origin)
                            }
                            ws.basicAuth = readBasicAuth(node)
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
                    connection.auth = readTls(connConfigNode)
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

    internal fun tryReadMethods(upNode: MappingNode): UpstreamsConfig.Methods? {
        return getMapping(upNode, "methods")?.let { mnode ->
            val enabled = getList<MappingNode>(mnode, "enabled")?.value?.map { m ->
                getValueAsString(m, "name")?.let { name ->
                    UpstreamsConfig.Method(
                            name = name
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
        return options
    }

    private fun readBasicAuth(node: MappingNode?): UpstreamsConfig.BasicAuth? {
        return getMapping(node, "basic-auth")?.let {  authNode ->
            val username = getValueAsString(authNode, "username")
            val password = getValueAsString(authNode, "password")
            if (username != null && password != null) {
                UpstreamsConfig.BasicAuth(username, password)
            } else {
                log.warn("Basic auth is not fully configured")
                null
            }
        }
    }

    private fun readTls(node: MappingNode?): UpstreamsConfig.TlsAuth? {
        return getMapping(node, "tls")?.let { authNode ->
            val auth = UpstreamsConfig.TlsAuth()
            auth.ca = getValueAsString(authNode, "ca")
            auth.certificate = getValueAsString(authNode, "certificate")
            auth.key = getValueAsString(authNode, "key")
            auth
        }
    }

}