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
package io.emeraldpay.dshackle.startup

import io.emeraldpay.dshackle.FileResolver
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.CurrentMultistreamHolder
import io.emeraldpay.dshackle.upstream.ForkWatchFactory
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.MergedHead
import io.emeraldpay.dshackle.upstream.bitcoin.BitcoinRpcHead
import io.emeraldpay.dshackle.upstream.bitcoin.BitcoinRpcUpstream
import io.emeraldpay.dshackle.upstream.bitcoin.BitcoinZMQHead
import io.emeraldpay.dshackle.upstream.bitcoin.EsploraClient
import io.emeraldpay.dshackle.upstream.bitcoin.ExtractBlock
import io.emeraldpay.dshackle.upstream.bitcoin.ZMQServer
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.calls.ManagedCallMethods
import io.emeraldpay.dshackle.upstream.ethereum.EthereumRpcUpstream
import io.emeraldpay.dshackle.upstream.ethereum.EthereumWsFactory
import io.emeraldpay.dshackle.upstream.ethereum.EthereumWsUpstream
import io.emeraldpay.dshackle.upstream.grpc.GrpcUpstreams
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcHttpClient
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.dshackle.upstream.rpcclient.RpcMetrics
import io.emeraldpay.grpc.BlockchainType
import io.emeraldpay.grpc.Chain
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Timer
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import java.net.URI
import java.util.concurrent.atomic.AtomicInteger
import javax.annotation.PostConstruct

@Repository
open class ConfiguredUpstreams(
    @Autowired private val currentUpstreams: CurrentMultistreamHolder,
    @Autowired private val fileResolver: FileResolver,
    @Autowired private val config: UpstreamsConfig,
) {

    private val log = LoggerFactory.getLogger(ConfiguredUpstreams::class.java)
    private var seq = AtomicInteger(0)

    private val forkWatchFactory = ForkWatchFactory(currentUpstreams)

    @PostConstruct
    fun start() {
        log.debug("Starting upstreams")
        val defaultOptions = buildDefaultOptions(config)
        config.upstreams.forEach { up ->
            if (!up.isEnabled) {
                log.debug("Upstream ${up.id} is disabled")
                return@forEach
            }
            log.debug("Start upstream ${up.id}")
            if (up.connection is UpstreamsConfig.GrpcConnection) {
                val options = up.options ?: UpstreamsConfig.Options()
                buildGrpcUpstream(up.cast(UpstreamsConfig.GrpcConnection::class.java), options)
            } else {
                val chain = Global.chainById(up.chain)
                if (chain == Chain.UNSPECIFIED) {
                    log.error("Chain is unknown: ${up.chain}")
                    return@forEach
                }
                val options = (up.options ?: UpstreamsConfig.Options())
                    .merge(defaultOptions[chain] ?: UpstreamsConfig.Options.getDefaults())
                when (BlockchainType.from(chain)) {
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
                Global.chainById(chainName).let { chain ->
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

    fun buildMethods(config: UpstreamsConfig.Upstream<*>, chain: Chain): CallMethods {
        return if (config.methods != null) {
            ManagedCallMethods(
                currentUpstreams.getDefaultMethods(chain),
                config.methods!!.enabled.map { it.name }.toSet(),
                config.methods!!.disabled.map { it.name }.toSet()
            ).also {
                config.methods!!.enabled.forEach { m ->
                    if (m.quorum != null) {
                        it.setQuorum(m.name, m.quorum)
                    }
                    if (m.static != null) {
                        it.setStaticResponse(m.name, m.static)
                    }
                }
            }
        } else {
            currentUpstreams.getDefaultMethods(chain)
        }
    }

    private fun buildBitcoinUpstream(
        config: UpstreamsConfig.Upstream<UpstreamsConfig.BitcoinConnection>,
        chain: Chain,
        options: UpstreamsConfig.Options
    ) {

        val conn = config.connection!!
        val directApi: Reader<JsonRpcRequest, JsonRpcResponse>? = buildHttpClient(config)
        if (directApi == null) {
            log.warn("Upstream doesn't have API configuration")
            return
        }

        val esplora = conn.esplora?.let { endpoint ->
            val tls = endpoint.tls?.let { tls ->
                tls.ca?.let { ca ->
                    fileResolver.resolve(ca).readBytes()
                }
            }
            EsploraClient(endpoint.url, endpoint.basicAuth, tls)
        }

        val extractBlock = ExtractBlock()
        val rpcHead = BitcoinRpcHead(directApi, extractBlock)
        val head: Head = conn.zeroMq?.let { zeroMq ->
            val server = ZMQServer(zeroMq.host, zeroMq.port, "hashblock")
            val zeroMqHead = BitcoinZMQHead(server, directApi, extractBlock)
            MergedHead(listOf(rpcHead, zeroMqHead))
        } ?: rpcHead

        val methods = buildMethods(config, chain)
        val upstream = BitcoinRpcUpstream(
            config.id
                ?: "bitcoin-${seq.getAndIncrement()}",
            chain, forkWatchFactory.create(chain), directApi, head,
            options, config.role,
            QuorumForLabels.QuorumItem(1, config.labels),
            methods, esplora
        )

        upstream.start()
        currentUpstreams.update(UpstreamChange(chain, upstream, UpstreamChange.ChangeType.ADDED))
    }

    private fun buildEthereumUpstream(
        config: UpstreamsConfig.Upstream<UpstreamsConfig.EthereumConnection>,
        chain: Chain,
        options: UpstreamsConfig.Options
    ) {
        val conn = config.connection!!

        val urls = ArrayList<URI>()
        val methods = buildMethods(config, chain)
        conn.rpc?.let { endpoint ->
            urls.add(endpoint.url)
        }

        val wsFactoryApi: EthereumWsFactory? = conn.ws?.let { endpoint ->
            val wsApi = EthereumWsFactory(
                config.id!!, chain,
                endpoint.url,
                endpoint.origin ?: URI("http://localhost"),
            )
            wsApi.config = endpoint
            endpoint.basicAuth?.let { auth ->
                wsApi.basicAuth = auth
            }
            urls.add(endpoint.url)
            wsApi
        }

        log.info("Using ${chain.chainName} upstream, at ${urls.joinToString()}")

        val directApi: Reader<JsonRpcRequest, JsonRpcResponse>? = buildHttpClient(config)
        if (directApi == null) {
            log.warn("Upstream doesn't have API configuration")
            return
        }

        val ethereumUpstream = if (wsFactoryApi != null && !conn.preferHttp) {
            EthereumWsUpstream(
                config.id!!,
                chain, forkWatchFactory.create(chain), directApi, wsFactoryApi,
                options, config.role,
                QuorumForLabels.QuorumItem(1, config.labels),
                methods
            )
        } else {
            EthereumRpcUpstream(
                config.id!!,
                chain, forkWatchFactory.create(chain), directApi, wsFactoryApi,
                options, config.role,
                QuorumForLabels.QuorumItem(1, config.labels),
                methods
            )
        }

        ethereumUpstream.start()
        currentUpstreams.update(UpstreamChange(chain, ethereumUpstream, UpstreamChange.ChangeType.ADDED))
    }

    private fun buildGrpcUpstream(
        config: UpstreamsConfig.Upstream<UpstreamsConfig.GrpcConnection>,
        options: UpstreamsConfig.Options
    ) {
        val endpoint = config.connection!!
        val ds = GrpcUpstreams(
            config.id!!,
            forkWatchFactory,
            config.role,
            endpoint.host!!,
            endpoint.port,
            endpoint.auth,
            fileResolver
        ).apply {
            this.options = options
        }
        log.info("Using ALL CHAINS (gRPC) upstream, at ${endpoint.host}:${endpoint.port}")
        ds.start()
            .doOnNext {
                log.info("Chain ${it.chain} ${it.type} through gRPC at ${endpoint.host}:${endpoint.port}. With caps: ${it.upstream.getCapabilities()}")
            }
            .subscribe(currentUpstreams::update)
    }

    private fun buildHttpClient(config: UpstreamsConfig.Upstream<out UpstreamsConfig.RpcConnection>): JsonRpcHttpClient? {
        val conn = config.connection!!
        val urls = ArrayList<URI>()
        return conn.rpc?.let { endpoint ->
            val tls = conn.rpc?.tls?.let { tls ->
                tls.ca?.let { ca ->
                    fileResolver.resolve(ca).readBytes()
                }
            }
            val metricsTags = listOf(
                // "unknown" is not supposed to happen
                Tag.of("upstream", config.id ?: "unknown"),
                // UNSPECIFIED shouldn't happen too
                Tag.of("chain", (Global.chainById(config.chain).chainCode))
            )
            val metrics = RpcMetrics(
                Timer.builder("upstream.rpc.conn")
                    .description("Request time through a HTTP JSON RPC connection")
                    .tags(metricsTags)
                    .publishPercentileHistogram()
                    .register(Metrics.globalRegistry),
                Counter.builder("upstream.rpc.fail")
                    .description("Number of failures of HTTP JSON RPC requests")
                    .tags(metricsTags)
                    .register(Metrics.globalRegistry)
            )
            urls.add(endpoint.url)
            JsonRpcHttpClient(
                endpoint.url.toString(),
                metrics,
                conn.rpc?.basicAuth,
                tls
            )
        }
    }
}
