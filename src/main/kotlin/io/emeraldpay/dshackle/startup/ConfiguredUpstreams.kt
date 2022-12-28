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

import com.google.common.annotations.VisibleForTesting
import io.emeraldpay.dshackle.BlockchainType
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.FileResolver
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.reader.JsonRpcReader
import io.emeraldpay.dshackle.upstream.*
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
import org.slf4j.LoggerFactory
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.context.ApplicationEventPublisher
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.net.URI
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Function
import kotlin.math.abs

@Component
open class ConfiguredUpstreams(
    private val fileResolver: FileResolver,
    private val config: UpstreamsConfig,
    private val callTargets: CallTargetsHolder,
    private val eventPublisher: ApplicationEventPublisher
) : ApplicationRunner {

    private val log = LoggerFactory.getLogger(ConfiguredUpstreams::class.java)
    private var seq = AtomicInteger(0)

    private val hashes: MutableMap<Byte, Boolean> = HashMap()

    override fun run(args: ApplicationArguments) {
        log.debug("Starting upstreams")
        val defaultOptions = buildDefaultOptions(config)
        val observerScheduler = Schedulers.newParallel("status-observer", 3)
        config.upstreams.forEach { up ->
            if (!up.isEnabled) {
                log.debug("Upstream ${up.id} is disabled")
                return@forEach
            }
            log.debug("Start upstream ${up.id}")
            if (up.connection is UpstreamsConfig.GrpcConnection) {
                val options = up.options ?: UpstreamsConfig.Options()
                buildGrpcUpstream(up.nodeId, up.cast(UpstreamsConfig.GrpcConnection::class.java), options)
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
                        buildEthereumUpstream(up.nodeId, up.cast(UpstreamsConfig.EthereumConnection::class.java), chain, options)
                    }

                    BlockchainType.BITCOIN -> {
                        buildBitcoinUpstream(up.cast(UpstreamsConfig.BitcoinConnection::class.java), chain, options)
                    }

                    BlockchainType.EVM_POS -> {
                        buildEthereumPosUpstream(
                            up.nodeId,
                            up.cast(UpstreamsConfig.EthereumPosConnection::class.java),
                            chain,
                            options
                        )
                    }
                }
                upstream?.let {
                    Flux.concat(Mono.just(UpstreamChangeEvent.ChangeType.ADDED), upstream.observeStatus())
                        .distinctUntilChanged()
                        .subscribeOn(observerScheduler)
                        .subscribe { status ->
                            when (status) {
                                UpstreamAvailability.UNAVAILABLE -> UpstreamChangeEvent.ChangeType.REMOVED
                                else -> UpstreamChangeEvent.ChangeType.REVALIDATED
                            }.let { eventType ->
                                if (eventType == UpstreamChangeEvent.ChangeType.REMOVED) {
                                    log.warn("Remove upstream ${it::class.java.simpleName}:${upstream.getId()} due to $status")
                                }
                                eventPublisher.publishEvent(UpstreamChangeEvent(chain, upstream, eventType))
                            }
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
        return if (config.methods != null || config.methodGroups != null) {
            ManagedCallMethods(
                delegate = callTargets.getDefaultMethods(chain),
                enabled = config.methods?.enabled?.map { it.name }?.toSet() ?: emptySet(),
                disabled = config.methods?.disabled?.map { it.name }?.toSet() ?: emptySet(),
                groupsEnabled = config.methodGroups?.enabled ?: emptySet(),
                groupsDisabled = config.methodGroups?.disabled ?: emptySet()
            ).also {
                config.methods?.enabled?.forEach { m ->
                    if (m.quorum != null) {
                        it.setQuorum(m.name, m.quorum)
                    }
                    if (m.static != null) {
                        it.setStaticResponse(m.name, m.static)
                    }
                }
            }
        } else {
            callTargets.getDefaultMethods(chain)
        }
    }

    private fun buildEthereumPosUpstream(
        nodeId: Int?,
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
        val connectorFactory = buildEthereumConnectorFactory(
            config.id!!,
            execution,
            chain,
            urls,
            NoChoiceWithPriorityForkChoice(conn.upstreamRating, config.id!!),
            BlockValidator.ALWAYS_VALID
        )
        val methods = buildMethods(config, chain)
        if (connectorFactory == null) {
            return null
        }

        val hashUrl = conn.execution!!.let {
            if (it.preferHttp) it.rpc?.url ?: it.ws?.url else it.ws?.url ?: it.rpc?.url
        }
        val hash = getHash(nodeId, hashUrl!!)
        val upstream = EthereumPosRpcUpstream(
            config.id!!,
            hash,
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
        val directApi: JsonRpcReader = httpFactory.create(config.id, chain)
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
        nodeId: Int?,
        config: UpstreamsConfig.Upstream<UpstreamsConfig.EthereumConnection>,
        chain: Chain,
        options: UpstreamsConfig.Options
    ): EthereumRpcUpstream? {
        val conn = config.connection!!

        val urls = ArrayList<URI>()
        val methods = buildMethods(config, chain)

        val connectorFactory = buildEthereumConnectorFactory(
            config.id!!,
            conn,
            chain,
            urls,
            MostWorkForkChoice(),
            EthereumBlockValidator()
        )
        if (connectorFactory == null) {
            return null
        }

        val hashUrl = if (conn.preferHttp) conn.rpc?.url ?: conn.ws?.url else conn.ws?.url ?: conn.rpc?.url
        val upstream = EthereumRpcUpstream(
            config.id!!,
            getHash(nodeId, hashUrl!!),
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
        nodeId: Int?,
        config: UpstreamsConfig.Upstream<UpstreamsConfig.GrpcConnection>,
        options: UpstreamsConfig.Options
    ) {
        val endpoint = config.connection!!
        val hash = getHash(nodeId, "${endpoint.host}:${endpoint.port}")
        val ds = GrpcUpstreams(
            config.id!!,
            hash,
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
            .subscribe(eventPublisher::publishEvent)
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

    private fun buildWsFactory(
        id: String,
        chain: Chain,
        conn: UpstreamsConfig.EthereumConnection,
        urls: ArrayList<URI>? = null
    ): EthereumWsFactory? {
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

    private fun buildEthereumConnectorFactory(
        id: String,
        conn: UpstreamsConfig.EthereumConnection,
        chain: Chain,
        urls: ArrayList<URI>,
        forkChoice: ForkChoice,
        blockValidator: BlockValidator
    ): EthereumConnectorFactory? {
        val wsFactoryApi = buildWsFactory(id, chain, conn, urls)
        val httpFactory = buildHttpFactory(conn, urls)
        log.info("Using ${chain.chainName} upstream, at ${urls.joinToString()}")
        val connectorFactory =
            EthereumConnectorFactory(conn.preferHttp, wsFactoryApi, httpFactory, forkChoice, blockValidator)
        if (!connectorFactory.isValid()) {
            log.warn("Upstream configuration is invalid (probably no http endpoint)")
            return null
        }
        return connectorFactory
    }

    @VisibleForTesting
    private fun getHash(nodeId: Int?, obj: Any): Byte =
        nodeId?.toByte() ?: (obj.hashCode() % 255).let {
            if (it == 0) 1 else it
        }.let { nonZeroHash ->
            listOf<Function<Int, Int>>(
                Function { i -> i },
                Function { i -> (-i) },
                Function { i -> 127 - abs(i) },
                Function { i -> abs(i) - 128 },
            ).map {
                it.apply(nonZeroHash).toByte()
            }.firstOrNull {
                hashes[it] != true
            }?.let {
                hashes[it] = true
                it
            } ?: (Byte.MIN_VALUE..Byte.MAX_VALUE).first {
                it != 0 && hashes[it.toByte()] != true
            }.toByte()
        }
}
