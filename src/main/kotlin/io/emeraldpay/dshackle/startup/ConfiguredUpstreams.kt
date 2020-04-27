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
package io.emeraldpay.dshackle.startup

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.BlockchainType
import io.emeraldpay.dshackle.FileResolver
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.upstream.CurrentUpstreams
import io.emeraldpay.dshackle.upstream.bitcoin.BitcoinApi
import io.emeraldpay.dshackle.upstream.bitcoin.BitcoinRpcClient
import io.emeraldpay.dshackle.upstream.bitcoin.BitcoinUpstream
import io.emeraldpay.dshackle.upstream.bitcoin.DefaultBitcoinMethods
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.calls.ManagedCallMethods
import io.emeraldpay.dshackle.upstream.ethereum.DirectEthereumApi
import io.emeraldpay.dshackle.upstream.ethereum.EthereumUpstream
import io.emeraldpay.dshackle.upstream.ethereum.EthereumWs
import io.emeraldpay.dshackle.upstream.grpc.GrpcUpstreams
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.rpc.http.ReactorHttpRpcClient
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import java.net.URI
import java.util.*
import java.util.concurrent.atomic.AtomicInteger
import javax.annotation.PostConstruct
import kotlin.collections.HashMap

@Repository
open class ConfiguredUpstreams(
        @Autowired private val objectMapper: ObjectMapper,
        @Autowired private val currentUpstreams: CurrentUpstreams,
        @Autowired private val fileResolver: FileResolver,
        @Autowired private val config: UpstreamsConfig
) {

    private val log = LoggerFactory.getLogger(ConfiguredUpstreams::class.java)
    private var seq = AtomicInteger(0)

    private val chainNames = mapOf(
            "ethereum" to Chain.ETHEREUM,
            "ethereum-classic" to Chain.ETHEREUM_CLASSIC,
            "eth" to Chain.ETHEREUM,
            "etc" to Chain.ETHEREUM_CLASSIC,
            "morden" to Chain.TESTNET_MORDEN,
            "kovan" to Chain.TESTNET_KOVAN,
            "kovan-testnet" to Chain.TESTNET_KOVAN,
            "bitcoin" to Chain.BITCOIN,
            "bitcoin-testnet" to Chain.TESTNET_BITCOIN
    )

    @PostConstruct
    fun start() {
        log.debug("Starting upstreams")
        val defaultOptions = buildDefaultOptions(config)
        config.upstreams.forEach { up ->
            log.debug("Start upstream ${up.id}")
            if (up.connection is UpstreamsConfig.GrpcConnection) {
                val options = up.options ?: UpstreamsConfig.Options()
                buildGrpcUpstream(up.cast(UpstreamsConfig.GrpcConnection::class.java), options)
            } else {
                val chain = chainNames[up.chain]
                if (chain == null) {
                    log.error("Chain is unknown: ${up.chain}")
                    return@forEach
                }
                val options = (up.options ?: UpstreamsConfig.Options())
                        .merge(defaultOptions[chain] ?: UpstreamsConfig.Options.getDefaults())
                when (BlockchainType.fromBlockchain(chain)) {
                    BlockchainType.ETHEREUM -> {
                        buildEthereumUpstream(up.cast(UpstreamsConfig.EthereumConnection::class.java), chain, options)
                    }
                    BlockchainType.BITCOIN -> {
                        buildBitcoinUpstream(up.cast(UpstreamsConfig.BitcoinConnection::class.java), chain, options)
                    }
                    else -> {
                        log.error("Chain is unsupported: ${up.chain}")
                        return@forEach
                    }
                }
            }
        }
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

    private fun buildMethods(config: UpstreamsConfig.Upstream<*>, chain: Chain): CallMethods {
        return if (config.methods != null) {
            ManagedCallMethods(currentUpstreams.getDefaultMethods(chain),
                    config.methods!!.enabled.map { it.name }.toSet(),
                    config.methods!!.disabled.map { it.name }.toSet()
            )
        } else {
            currentUpstreams.getDefaultMethods(chain)
        }
    }

    private fun buildBitcoinUpstream(config: UpstreamsConfig.Upstream<UpstreamsConfig.BitcoinConnection>,
                                     chain: Chain,
                                     options: UpstreamsConfig.Options) {

        val conn = config.connection!!
        var rpcApi: BitcoinApi? = null
        val methods = buildMethods(config, chain)
        conn.rpc?.let { endpoint ->
            val rpcClient = BitcoinRpcClient(endpoint.url.toString(), endpoint.basicAuth!!)
            rpcApi = BitcoinApi(rpcClient, objectMapper, methods)
        }
        rpcApi?.let { api ->
            val upstream = BitcoinUpstream(config.id
                    ?: "bitcoin-${seq.getAndIncrement()}", chain, api,
                    options, QuorumForLabels.QuorumItem(1, config.labels),
                    objectMapper, methods)

            upstream.start()
            currentUpstreams.update(UpstreamChange(chain, upstream, UpstreamChange.ChangeType.ADDED))
        }

    }

    private fun buildEthereumUpstream(config: UpstreamsConfig.Upstream<UpstreamsConfig.EthereumConnection>,
                                      chain: Chain,
                                      options: UpstreamsConfig.Options) {
        val conn = config.connection!!
        var rpcApi: DirectEthereumApi? = null
        val urls = ArrayList<URI>()
        val methods = buildMethods(config, chain)
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
                        rpcApi!!,
                        objectMapper
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
                    QuorumForLabels.QuorumItem(1, config.labels),
                    methods,
                    objectMapper)
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