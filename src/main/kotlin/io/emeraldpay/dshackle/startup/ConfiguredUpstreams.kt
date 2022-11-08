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
import io.emeraldpay.dshackle.upstream.BlockValidator
import io.emeraldpay.dshackle.upstream.CurrentMultistreamHolder
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.HttpRpcFactory
import io.emeraldpay.dshackle.upstream.MergedHead
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.bitcoin.BitcoinRpcHead
import io.emeraldpay.dshackle.upstream.bitcoin.BitcoinRpcUpstream
import io.emeraldpay.dshackle.upstream.bitcoin.BitcoinZMQHead
import io.emeraldpay.dshackle.upstream.bitcoin.EsploraClient
import io.emeraldpay.dshackle.upstream.bitcoin.ExtractBlock
import io.emeraldpay.dshackle.upstream.bitcoin.ZMQServer
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.calls.ManagedCallMethods
import io.emeraldpay.dshackle.upstream.ethereum.EthereumBlockValidator
import io.emeraldpay.dshackle.upstream.ethereum.EthereumPosRpcUpstream
import io.emeraldpay.dshackle.upstream.ethereum.EthereumRpcUpstream
import io.emeraldpay.dshackle.upstream.ethereum.EthereumWsFactory
import io.emeraldpay.dshackle.upstream.ethereum.connectors.EthereumConnectorFactory
import io.emeraldpay.dshackle.upstream.forkchoice.ForkChoice
import io.emeraldpay.dshackle.upstream.forkchoice.MostWorkForkChoice
import io.emeraldpay.dshackle.upstream.forkchoice.NoChoiceWithPriorityForkChoice
import io.emeraldpay.dshackle.upstream.grpc.GrpcUpstreams
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.grpc.BlockchainType
import io.emeraldpay.grpc.Chain
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
    @Autowired private val config: UpstreamsConfig
) {

    private val log = LoggerFactory.getLogger(ConfiguredUpstreams::class.java)
    private var seq = AtomicInteger(0)

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
                val upstream = when (BlockchainType.from(chain)) {
                    BlockchainType.EVM_POW -> {
                        buildEthereumUpstream(up.cast(UpstreamsConfig.EthereumConnection::class.java), chain, options)
                    }
                    BlockchainType.BITCOIN -> {
                        buildBitcoinUpstream(up.cast(UpstreamsConfig.BitcoinConnection::class.java), chain, options)
                    }
                    BlockchainType.EVM_POS -> {
                        buildEthereumPosUpstream(up.cast(UpstreamsConfig.EthereumPosConnection::class.java), chain, options)
                    }
                    else -> {
                        log.error("Chain is unsupported: ${up.chain}")
                        return@forEach
                    }
                }
                upstream?.let {
                    currentUpstreams.update(UpstreamChange(chain, upstream, UpstreamChange.ChangeType.ADDED))
                }
            }
        }
    }

    fun hasMatchingUpstream(chain: Chain, matcher: Selector.LabelSelectorMatcher): Boolean =
        config.upstreams.any { up ->
            (up.chain?.let { Global.chainById(it) == chain } ?: true) && matcher.matches(up.labels)
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

    private fun buildEthereumPosUpstream(
        config: UpstreamsConfig.Upstream<UpstreamsConfig.EthereumPosConnection>,
        chain: Chain,
        options: UpstreamsConfig.Options
    ): Upstream? {
        val conn = config.connection!!
        val execution = conn.execution
        if (execution == null) {
            log.warn("Upstream doesn't have execution layer configuration")
            return null
        }
        val urls = ArrayList<URI>()
        val connectorFactory = buildEthereumConnectorFactory(config.id!!, execution, chain, urls, NoChoiceWithPriorityForkChoice(conn.upstreamRating), BlockValidator.ALWAYS_VALID)
        val methods = buildMethods(config, chain)
        if (connectorFactory == null) {
            return null
        }
        val upstream = EthereumPosRpcUpstream(
            config.id!!,
            chain,
            options, config.role,
            methods,
            QuorumForLabels.QuorumItem(1, config.labels),
            connectorFactory
        )
        upstream.start()
        return upstream
    }

    private fun buildBitcoinUpstream(
        config: UpstreamsConfig.Upstream<UpstreamsConfig.BitcoinConnection>,
        chain: Chain,
        options: UpstreamsConfig.Options
    ): Upstream? {
        val conn = config.connection!!
        val httpFactory = buildHttpFactory(conn)
        if (httpFactory == null) {
            log.warn("Upstream doesn't have API configuration")
            return null
        }
        val directApi: Reader<JsonRpcRequest, JsonRpcResponse> = httpFactory.create(config.id, chain)
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
            MergedHead(listOf(rpcHead, zeroMqHead), MostWorkForkChoice())
        } ?: rpcHead

        val methods = buildMethods(config, chain)
        val upstream = BitcoinRpcUpstream(
            config.id
                ?: "bitcoin-${seq.getAndIncrement()}",
            chain, directApi, head,
            options, config.role,
            QuorumForLabels.QuorumItem(1, config.labels),
            methods, esplora
        )
        upstream.start()
        return upstream
    }

    private fun buildEthereumUpstream(
        config: UpstreamsConfig.Upstream<UpstreamsConfig.EthereumConnection>,
        chain: Chain,
        options: UpstreamsConfig.Options
    ): EthereumRpcUpstream? {
        val conn = config.connection!!

        val urls = ArrayList<URI>()
        val methods = buildMethods(config, chain)

        val connectorFactory = buildEthereumConnectorFactory(config.id!!, conn, chain, urls, MostWorkForkChoice(), EthereumBlockValidator())
        if (connectorFactory == null) {
            return null
        }
        val upstream = EthereumRpcUpstream(
            config.id!!,
            chain,
            options, config.role,
            methods,
            QuorumForLabels.QuorumItem(1, config.labels),
            connectorFactory
        )
        upstream.start()
        return upstream
    }

    private fun buildGrpcUpstream(
        config: UpstreamsConfig.Upstream<UpstreamsConfig.GrpcConnection>,
        options: UpstreamsConfig.Options
    ) {
        val endpoint = config.connection!!
        val ds = GrpcUpstreams(
            config.id!!,
            config.role,
            endpoint.host!!,
            endpoint.port,
            endpoint.auth,
            fileResolver,
            endpoint.upstreamRating,
            config.labels
        ).apply {
            timeout = options.timeout
        }
        log.info("Using ALL CHAINS (gRPC) upstream, at ${endpoint.host}:${endpoint.port}")
        ds.start()
            .doOnNext {
                log.info("Chain ${it.chain} ${it.type} through gRPC at ${endpoint.host}:${endpoint.port}. With caps: ${it.upstream.getCapabilities()}")
            }
            .subscribe(currentUpstreams::update)
    }

    private fun buildHttpFactory(conn: UpstreamsConfig.RpcConnection, urls: ArrayList<URI>? = null): HttpRpcFactory? {
        return conn.rpc?.let { endpoint ->
            val tls = conn.rpc?.tls?.let { tls ->
                tls.ca?.let { ca ->
                    fileResolver.resolve(ca).readBytes()
                }
            }
            urls?.add(endpoint.url)
            HttpRpcFactory(endpoint.url.toString(), conn.rpc?.basicAuth, tls)
        }
    }

    private fun buildWsFactory(id: String, chain: Chain, conn: UpstreamsConfig.EthereumConnection, urls: ArrayList<URI>? = null): EthereumWsFactory? {
        return conn.ws?.let { endpoint ->
            val wsApi = EthereumWsFactory(
                id, chain,
                endpoint.url,
                endpoint.origin ?: URI("http://localhost"),
            )
            wsApi.config = endpoint
            endpoint.basicAuth?.let { auth ->
                wsApi.basicAuth = auth
            }
            urls?.add(endpoint.url)
            wsApi
        }
    }

    private fun buildEthereumConnectorFactory(id: String, conn: UpstreamsConfig.EthereumConnection, chain: Chain, urls: ArrayList<URI>, forkChoice: ForkChoice, blockValidator: BlockValidator): EthereumConnectorFactory? {
        val wsFactoryApi = buildWsFactory(id, chain, conn, urls)
        val httpFactory = buildHttpFactory(conn, urls)
        log.info("Using ${chain.chainName} upstream, at ${urls.joinToString()}")
        val connectorFactory = EthereumConnectorFactory(conn.preferHttp, wsFactoryApi, httpFactory, forkChoice, blockValidator)
        if (!connectorFactory.isValid()) {
            log.warn("Upstream configuration is invalid (probably no http endpoint)")
            return null
        }
        return connectorFactory
    }
}
