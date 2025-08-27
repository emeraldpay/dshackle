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

import io.emeraldpay.api.BlockchainType
import io.emeraldpay.api.Chain
import io.emeraldpay.dshackle.FileResolver
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.cache.CachesFactory
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.monitoring.Channel
import io.emeraldpay.dshackle.monitoring.requestlog.CurrentRequestLogWriter
import io.emeraldpay.dshackle.reader.StandardRpcReader
import io.emeraldpay.dshackle.upstream.CurrentMultistreamHolder
import io.emeraldpay.dshackle.upstream.ForkWatchFactory
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.MergedPowHead
import io.emeraldpay.dshackle.upstream.bitcoin.BitcoinCacheUpdate
import io.emeraldpay.dshackle.upstream.bitcoin.BitcoinRpcHead
import io.emeraldpay.dshackle.upstream.bitcoin.BitcoinRpcUpstream
import io.emeraldpay.dshackle.upstream.bitcoin.BitcoinZMQHead
import io.emeraldpay.dshackle.upstream.bitcoin.EsploraClient
import io.emeraldpay.dshackle.upstream.bitcoin.ExtractBlock
import io.emeraldpay.dshackle.upstream.bitcoin.ZMQServer
import io.emeraldpay.dshackle.upstream.bitcoin.subscribe.BitcoinRpcIngressSubscription
import io.emeraldpay.dshackle.upstream.bitcoin.subscribe.BitcoinZmqSubscriptionSource
import io.emeraldpay.dshackle.upstream.bitcoin.subscribe.BitcoinZmqTopic
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.calls.ManagedCallMethods
import io.emeraldpay.dshackle.upstream.ethereum.CacheUpdate
import io.emeraldpay.dshackle.upstream.ethereum.ConnectionMetrics
import io.emeraldpay.dshackle.upstream.ethereum.EthereumRpcUpstream
import io.emeraldpay.dshackle.upstream.ethereum.EthereumUpstream
import io.emeraldpay.dshackle.upstream.ethereum.EthereumWsFactory
import io.emeraldpay.dshackle.upstream.ethereum.EthereumWsUpstream
import io.emeraldpay.dshackle.upstream.ethereum.WsConnectionMultiPool
import io.emeraldpay.dshackle.upstream.ethereum.WsConnectionSinglePool
import io.emeraldpay.dshackle.upstream.grpc.GrpcUpstreams
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcHttpClient
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcSwitchClient
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcWsClient
import io.emeraldpay.dshackle.upstream.rpcclient.RpcMetrics
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Timer
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import java.net.URI
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Consumer
import javax.annotation.PostConstruct

@Repository
open class ConfiguredUpstreams(
    @Autowired private val currentUpstreams: CurrentMultistreamHolder,
    @Autowired private val fileResolver: FileResolver,
    @Autowired private val config: UpstreamsConfig,
    @Autowired private val currentRequestLogWriter: CurrentRequestLogWriter,
    @Autowired private val cachesFactory: CachesFactory,
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
                val options = up.options ?: UpstreamsConfig.PartialOptions()
                buildGrpcUpstream(up.cast(UpstreamsConfig.GrpcConnection::class.java), options.build())
            } else {
                val chain = Global.chainById(up.blockchain)
                if (chain == Chain.UNSPECIFIED) {
                    log.error("Chain is unknown: ${up.blockchain}")
                    return@forEach
                }
                val options =
                    (defaultOptions[chain] ?: UpstreamsConfig.PartialOptions.getDefaults())
                        .merge(up.options ?: UpstreamsConfig.PartialOptions())
                when (BlockchainType.from(chain)) {
                    BlockchainType.ETHEREUM -> {
                        buildEthereumUpstream(up.cast(UpstreamsConfig.EthereumConnection::class.java), chain, options.build())
                    }
                    BlockchainType.BITCOIN -> {
                        buildBitcoinUpstream(up.cast(UpstreamsConfig.BitcoinConnection::class.java), chain, options.build())
                    }
                    else -> {
                        log.error("Chain is unsupported: ${up.blockchain}")
                        return@forEach
                    }
                }
            }
        }
    }

    private fun buildDefaultOptions(config: UpstreamsConfig): HashMap<Chain, UpstreamsConfig.PartialOptions> {
        val defaultOptions = HashMap<Chain, UpstreamsConfig.PartialOptions>()
        config.defaultOptions.forEach { defaultsConfig ->
            defaultsConfig.blockchains?.forEach { chainName ->
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
        return defaultOptions
    }

    fun buildMethods(
        config: UpstreamsConfig.Upstream<*>,
        chain: Chain,
    ): CallMethods =
        if (config.methods != null) {
            ManagedCallMethods(
                currentUpstreams.getDefaultMethods(chain),
                config.methods!!
                    .enabled
                    .map { it.name }
                    .toSet(),
                config.methods!!
                    .disabled
                    .map { it.name }
                    .toSet(),
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

    private fun buildBitcoinUpstream(
        config: UpstreamsConfig.Upstream<UpstreamsConfig.BitcoinConnection>,
        chain: Chain,
        options: UpstreamsConfig.Options,
    ) {
        val id = config.id ?: "bitcoin-${seq.getAndIncrement()}"
        val conn = config.connection!!
        val directApi: StandardRpcReader? =
            buildHttpClient(config)
                ?.let {
                    currentRequestLogWriter.wrap(it, id, Channel.JSONRPC)
                }?.let {
                    BitcoinCacheUpdate(cachesFactory.getCaches(chain), it)
                }
        if (directApi == null) {
            log.warn("Upstream doesn't have API configuration")
            return
        }

        val esplora =
            conn.esplora?.let { endpoint ->
                val tls =
                    endpoint.tls?.let { tls ->
                        tls.ca?.let { ca ->
                            fileResolver.resolve(ca).readBytes()
                        }
                    }
                EsploraClient(endpoint.url, endpoint.basicAuth, tls)
            }

        val extractBlock = ExtractBlock()
        val rpcHead = BitcoinRpcHead(directApi, extractBlock)
        val head: Head =
            conn.zeroMq?.let { zeroMq ->
                val server = ZMQServer(zeroMq.host, zeroMq.port, "hashblock")
                val zeroMqHead = BitcoinZMQHead(server, directApi, extractBlock)
                MergedPowHead(listOf(rpcHead, zeroMqHead))
            } ?: rpcHead

        val subscriptions =
            conn.zeroMq?.let { zeroMq ->
                zeroMq.topics.mapNotNull {
                    val topic = BitcoinZmqTopic.findById(it)
                    if (topic == null) {
                        log.error("ZeroMQ topic is unknown: $it")
                        return@mapNotNull null
                    }
                    val server = ZMQServer(zeroMq.host, zeroMq.port, topic.id)
                    return@mapNotNull BitcoinZmqSubscriptionSource(topic, server)
                }
            }

        val methods = buildMethods(config, chain)
        val upstream =
            BitcoinRpcUpstream(
                id,
                chain,
                forkWatchFactory.create(chain),
                directApi,
                head,
                options,
                config.role,
                QuorumForLabels.QuorumItem(1, config.labels),
                methods,
                BitcoinRpcIngressSubscription(subscriptions.orEmpty()),
                esplora,
            )

        upstream.start()
        currentUpstreams.update(UpstreamChange(chain, upstream, UpstreamChange.ChangeType.ADDED))
    }

    private fun buildEthereumUpstream(
        config: UpstreamsConfig.Upstream<UpstreamsConfig.EthereumConnection>,
        chain: Chain,
        options: UpstreamsConfig.Options,
    ) {
        val id = config.id ?: "ethereum-${seq.getAndIncrement()}"
        val conn = config.connection!!

        val urls = ArrayList<URI>()
        val methods = buildMethods(config, chain)
        conn.rpc?.let { endpoint ->
            urls.add(endpoint.url)
        }

        val wsFactoryApi: EthereumWsFactory? =
            conn.ws?.let { endpoint ->
                val wsApi =
                    EthereumWsFactory(
                        id,
                        chain,
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

        val httpApi: StandardRpcReader? = buildHttpClient(config)
        val wsPool =
            wsFactoryApi?.let { factory ->
                val connection = config.connection?.ws?.connections ?: 1
                if (connection > 1) {
                    WsConnectionMultiPool(factory, connection)
                } else {
                    WsConnectionSinglePool(factory.create(null))
                }
            }

        val ethereumUpstream: EthereumUpstream? =
            if (httpApi != null && wsPool != null) {
                if (conn.preferHttp) {
                    val directReader =
                        httpApi.let {
                            currentRequestLogWriter.wrap(it, id, Channel.JSONRPC)
                        }
                    EthereumRpcUpstream(
                        id,
                        chain,
                        forkWatchFactory.create(chain),
                        CacheUpdate(cachesFactory.getCaches(chain), directReader),
                        wsPool,
                        options,
                        config.role,
                        QuorumForLabels.QuorumItem(1, config.labels),
                        methods,
                    )
                } else {
                    // Sometimes the server may close the WebSocket connection during the execution of a call, for example if the response
                    // is too large for WebSockets Frame (and Geth is unable to split messages into separate frames)
                    // In this case the failed request must be rerouted to the HTTP connection, because otherwise it would always fail
                    val directReader =
                        JsonRpcSwitchClient(
                            JsonRpcWsClient(wsPool, emptyOnNoConnection = true).let {
                                currentRequestLogWriter.wrap(it, id, Channel.WSJSONRPC)
                            },
                            httpApi.let {
                                currentRequestLogWriter.wrap(it, id, Channel.JSONRPC)
                            },
                        )
                    EthereumWsUpstream(
                        id,
                        chain,
                        forkWatchFactory.create(chain),
                        CacheUpdate(cachesFactory.getCaches(chain), directReader),
                        wsPool,
                        options,
                        config.role,
                        QuorumForLabels.QuorumItem(1, config.labels),
                        methods,
                    ).also { upstream ->
                        // pass WS connection status to the upstream itself
                        wsPool.statusUpdates = Consumer(upstream::setStatus)
                    }
                }
            } else if (httpApi != null) {
                val directReader =
                    httpApi.let {
                        currentRequestLogWriter.wrap(it, id, Channel.JSONRPC)
                    }
                EthereumRpcUpstream(
                    id,
                    chain,
                    forkWatchFactory.create(chain),
                    CacheUpdate(cachesFactory.getCaches(chain), directReader),
                    null,
                    options,
                    config.role,
                    QuorumForLabels.QuorumItem(1, config.labels),
                    methods,
                )
            } else if (wsPool != null) {
                val directReader =
                    JsonRpcWsClient(wsPool, emptyOnNoConnection = false).let {
                        currentRequestLogWriter.wrap(it, id, Channel.WSJSONRPC)
                    }
                EthereumWsUpstream(
                    id,
                    chain,
                    forkWatchFactory.create(chain),
                    CacheUpdate(cachesFactory.getCaches(chain), directReader),
                    wsPool,
                    options,
                    config.role,
                    QuorumForLabels.QuorumItem(1, config.labels),
                    methods,
                )
            } else {
                null
            }

        if (ethereumUpstream == null) {
            log.error("No connection is configured for upstream $id")
            return
        }

        wsPool?.connect()
        ethereumUpstream.start()
        currentUpstreams.update(UpstreamChange(chain, ethereumUpstream, UpstreamChange.ChangeType.ADDED))
    }

    private fun buildGrpcUpstream(
        config: UpstreamsConfig.Upstream<UpstreamsConfig.GrpcConnection>,
        options: UpstreamsConfig.Options,
    ) {
        val endpoint = config.connection!!
        val ds =
            GrpcUpstreams(
                config.id!!,
                forkWatchFactory,
                config.role,
                endpoint,
                fileResolver,
                currentRequestLogWriter,
            ).apply {
                this.options = options
                endpoint.compress?.let { value ->
                    this.compress = value
                }
            }
        log.info("Using ALL CHAINS (gRPC) upstream, at ${endpoint.host}:${endpoint.port}")
        ds
            .subscribeUpstreamChanges()
            .doOnNext {
                log.info(
                    "Chain ${it.chain} ${it.type} through gRPC at ${endpoint.host}:${endpoint.port}. With caps: ${it.upstream.getCapabilities()}",
                )
            }.subscribe(currentUpstreams::update)
        ds.startStatusUpdates()
    }

    private fun buildHttpClient(config: UpstreamsConfig.Upstream<out UpstreamsConfig.RpcConnection>): JsonRpcHttpClient? {
        val conn = config.connection!!
        val urls = ArrayList<URI>()
        return conn.rpc?.let { endpoint ->
            val tls =
                conn.rpc?.tls?.let { tls ->
                    tls.ca?.let { ca ->
                        fileResolver.resolve(ca).readBytes()
                    }
                }
            val metricsTags =
                listOf(
                    // "unknown" is not supposed to happen
                    Tag.of("upstream", config.id ?: "unknown"),
                    // UNSPECIFIED shouldn't happen too
                    Tag.of("chain", (Global.chainById(config.blockchain).chainCode)),
                )
            val metrics =
                RpcMetrics(
                    metricsTags,
                    timer =
                        Timer
                            .builder("upstream.rpc.conn")
                            .description("Request time through a HTTP JSON RPC connection")
                            .tags(metricsTags)
                            .publishPercentileHistogram()
                            .register(Metrics.globalRegistry),
                    fails =
                        Counter
                            .builder("upstream.rpc.fail")
                            .description("Number of failures of HTTP JSON RPC requests")
                            .tags(metricsTags)
                            .register(Metrics.globalRegistry),
                    responseSize =
                        DistributionSummary
                            .builder("upstream.rpc.response.size")
                            .description("Size of HTTP JSON RPC responses")
                            .tags(metricsTags)
                            .register(Metrics.globalRegistry),
                    connectionMetrics = ConnectionMetrics(metricsTags),
                )
            urls.add(endpoint.url)
            val client =
                JsonRpcHttpClient(
                    endpoint.url.toString(),
                    metrics,
                    conn.rpc?.basicAuth,
                    tls,
                )
            conn.rpc?.compress?.let { value ->
                client.compress = value
            }
            client
        }
    }
}
