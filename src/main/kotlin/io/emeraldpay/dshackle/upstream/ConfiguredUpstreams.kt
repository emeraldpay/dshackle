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
package io.emeraldpay.dshackle.upstream

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.FileResolver
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.config.UpstreamsConfigReader
import io.emeraldpay.dshackle.upstream.ethereum.DirectEthereumApi
import io.emeraldpay.dshackle.upstream.ethereum.EthereumUpstream
import io.emeraldpay.dshackle.upstream.ethereum.EthereumWs
import io.emeraldpay.dshackle.upstream.grpc.GrpcUpstreams
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.rpc.http.ReactorHttpRpcClient
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.env.Environment
import org.springframework.stereotype.Repository
import java.net.URI
import java.util.*
import javax.annotation.PostConstruct
import kotlin.collections.HashMap
import kotlin.system.exitProcess

@Repository
open class ConfiguredUpstreams(
        @Autowired val env: Environment,
        @Autowired private val objectMapper: ObjectMapper,
        @Autowired private val currentUpstreams: CurrentUpstreams,
        @Autowired private val fileResolver: FileResolver
) {

    private val log = LoggerFactory.getLogger(ConfiguredUpstreams::class.java)

    private val chainNames = mapOf(
            "ethereum" to Chain.ETHEREUM,
            "ethereum-classic" to Chain.ETHEREUM_CLASSIC,
            "eth" to Chain.ETHEREUM,
            "etc" to Chain.ETHEREUM_CLASSIC,
            "morden" to Chain.TESTNET_MORDEN,
            "kovan" to Chain.TESTNET_KOVAN
    )

    @PostConstruct
    fun start() {
        val config = readConfig()
        val defaultOptions = buildDefaultOptions(config)
        config.upstreams.forEach { up ->

            if (up.connection is UpstreamsConfig.GrpcConnection) {
                val options = up.options ?: UpstreamsConfig.Options()
                buildGrpcUpstream(up as UpstreamsConfig.Upstream<UpstreamsConfig.GrpcConnection>, options)
            } else {
                val chain = chainNames[up.chain]
                if (chain == null) {
                    log.error("Chain not supported: ${up.chain}")
                    return@forEach
                }
                val options = (up.options ?: UpstreamsConfig.Options())
                        .merge(defaultOptions[chain] ?: UpstreamsConfig.Options.getDefaults())
                buildEthereumUpstream(up as UpstreamsConfig.Upstream<UpstreamsConfig.EthereumConnection>, chain, options)
            }
        }
    }

    private fun readConfig(): UpstreamsConfig {
        val path = env.getProperty("upstreams.config")
        if (StringUtils.isEmpty(path)) {
            log.error("Path to upstreams is not set (upstreams.config)")
            exitProcess(1)
        }
        val upstreamConfig = fileResolver.resolve(path!!).normalize()
        val ok = upstreamConfig.exists() && upstreamConfig.isFile
        if (!ok) {
            log.error("Unable to setup upstreams from ${upstreamConfig.path}")
            exitProcess(1)
        }
        log.info("Read upstream configuration from ${upstreamConfig.path}")
        val reader = UpstreamsConfigReader()
        return reader.read(upstreamConfig.inputStream())
    }

    private fun buildDefaultOptions(config: UpstreamsConfig): HashMap<Chain, UpstreamsConfig.Options> {
        val defaultOptions = HashMap<Chain, UpstreamsConfig.Options>()
        config.defaultOptions.forEach { defaultsConfig ->
            defaultsConfig.chains?.forEach { chainName ->
                chainNames[chainName]?.let { chain ->
                    defaultsConfig.options?.let { options ->
                        if (!defaultOptions.containsKey(chain)) {
                            defaultOptions[chain] = options
                        } else {
                            defaultOptions[chain] = defaultOptions[chain]!!.merge(options)
                        }
                    }
                }
            }
        }
        defaultOptions.keys.forEach { chain ->
            defaultOptions[chain] = defaultOptions[chain]!!.merge(UpstreamsConfig.Options.getDefaults())
        }
        return defaultOptions
    }

    private fun buildEthereumUpstream(config: UpstreamsConfig.Upstream<UpstreamsConfig.EthereumConnection>,
                                      chain: Chain,
                                      options: UpstreamsConfig.Options
                                      ) {
        val conn = config.connection!!
        var rpcApi: DirectEthereumApi? = null
        val urls = ArrayList<URI>()
        val methods = if (config.methods != null) {
            ManagedCallMethods(currentUpstreams.getDefaultMethods(chain),
                    config.methods!!.enabled.map { it.name }.toSet(),
                    config.methods!!.disabled.map { it.name }.toSet()
            )
        } else {
            currentUpstreams.getDefaultMethods(chain)
        }
        conn.rpc?.let { endpoint ->
            val rpcClient = ReactorHttpRpcClient.newBuilder()
                    .connectTo(endpoint.url)
                    .alwaysSeparate()
            conn.rpc?.basicAuth?.let { auth ->
                rpcClient.basicAuth(auth.username, auth.password)
            }
            conn.rpc?.tls?.let { tls ->
                tls.ca?.let { ca ->
                    fileResolver.resolve(ca).inputStream().use { cert -> rpcClient.trustedCertificate(cert) }
                }
            }
            rpcApi = DirectEthereumApi(
                    rpcClient.build(),
                    null,
                    objectMapper,
                    methods
            ).apply {
                timeout = options.timeout
            }

            urls.add(endpoint.url)
        }
        if (rpcApi != null) {
            val wsApi: EthereumWs? = conn.ws?.let { endpoint ->
                val wsApi = EthereumWs(
                        endpoint.url,
                        endpoint.origin ?: URI("http://localhost"),
                        rpcApi!!
                )
                endpoint.basicAuth?.let { auth ->
                    wsApi.basicAuth = auth
                }
                wsApi.connect()
                urls.add(endpoint.url)
                wsApi
            }

            log.info("Using ${chain.chainName} upstream, at ${urls.joinToString()}")
            val ethereumUpstream = EthereumUpstream(
                    config.id!!,
                    chain, rpcApi!!, wsApi, options,
                    NodeDetailsList.NodeDetails(1, config.labels),
                    methods)
            ethereumUpstream.start()
            currentUpstreams.update(UpstreamChange(chain, ethereumUpstream, UpstreamChange.ChangeType.ADDED))
        }
    }

    private fun buildGrpcUpstream(config: UpstreamsConfig.Upstream<UpstreamsConfig.GrpcConnection>, options: UpstreamsConfig.Options) {
        val endpoint = config.connection!!
        val ds = GrpcUpstreams(
                config.id!!,
                endpoint.host!!,
                endpoint.port ?: 2449,
                objectMapper,
                endpoint.auth,
                fileResolver
        ).apply {
            timeout = options.timeout
        }
        log.info("Using ALL CHAINS (gRPC) upstream, at ${endpoint.host}:${endpoint.port}")
        ds.start()
                .doOnNext {
                    log.info("Chain ${it.chain} has ${it.type} through gRPC at ${endpoint.host}:${endpoint.port}")
                }
                .subscribe(currentUpstreams::update)
    }


}