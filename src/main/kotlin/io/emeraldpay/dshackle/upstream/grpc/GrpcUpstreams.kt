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
package io.emeraldpay.dshackle.upstream.grpc

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.ReactorBlockchainGrpc
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.upstream.Upstreams
import io.emeraldpay.grpc.Chain
import io.grpc.ManagedChannelBuilder
import io.grpc.netty.NettyChannelBuilder
import io.netty.handler.ssl.*
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.toFlux
import reactor.util.function.Tuple2
import reactor.util.function.Tuples
import java.io.File
import java.util.*
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class GrpcUpstreams(
        private val host: String,
        private val port: Int,
        private val objectMapper: ObjectMapper,
        private val auth: UpstreamsConfig.TlsAuth? = null
) {
    private val log = LoggerFactory.getLogger(GrpcUpstreams::class.java)

    private var client: ReactorBlockchainGrpc.ReactorBlockchainStub? = null
    private var known = HashMap<Chain, GrpcUpstream>()
    private val lock = ReentrantLock()

    fun start(): Flux<Tuple2<Chain, GrpcUpstream>> {
        val channel: ManagedChannelBuilder<*> = if (auth != null && StringUtils.isNotEmpty(auth.ca)) {
            NettyChannelBuilder.forAddress(host, port)
                    .useTransportSecurity()
                    .sslContext(withTls(auth))
        } else {
            log.warn("Using insecure connection for $host:$port")
            ManagedChannelBuilder.forAddress(host, port)
                    .usePlaintext()
        }

        val client = ReactorBlockchainGrpc.newReactorStub(channel.build())
        this.client = client
        val loaded = client.describe(BlockchainOuterClass.DescribeRequest.newBuilder().build())
                .map { value ->
                    val chains = ArrayList<Chain>()
                    value.chainsList.filter {
                        Chain.byId(it.chain.number) != Chain.UNSPECIFIED
                    }.map { chainDetails ->
                        val chain = Chain.byId(chainDetails.chain.number)
                        val up = getOrCreate(chain)
                        up.init(chainDetails)
                        chains.add(chain)
                        Tuples.of(chain, up)
                    }
                }.flatMapMany {
                    it.toFlux()
                }.doOnError { t ->
                    log.error("Failed to get description from $host:$port", t)
                }
        //TODO subscribe only after receiving details
        client.subscribeStatus(BlockchainOuterClass.StatusRequest.newBuilder().build())
                .subscribe { value ->
                    val chain = Chain.byId(value.chain.number)
                    if (chain != Chain.UNSPECIFIED) {
                        known[chain]?.onStatus(value)
                    }
                }
        return loaded
    }

    internal fun withTls(auth: UpstreamsConfig.TlsAuth): SslContext {
        val sslContext = SslContextBuilder.forClient()
                .clientAuth(ClientAuth.REQUIRE)
        sslContext.trustManager(File(auth.ca!!).inputStream())
        if (StringUtils.isNotEmpty(auth.key) && StringUtils.isNoneEmpty(auth.certificate)) {
            sslContext.keyManager(File(auth.certificate!!).inputStream(), File(auth.key!!).inputStream())
        } else {
            log.warn("Connect to remote using only CA certificate")
        }
        val alpn = ApplicationProtocolConfig(
                ApplicationProtocolConfig.Protocol.ALPN,
                ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                "grpc-exp", "h2")
        sslContext.applicationProtocolConfig(alpn)
        return sslContext.build()
    }

    fun getOrCreate(chain: Chain): GrpcUpstream {
        lock.withLock {
            val current = known[chain]
            return if (current == null) {
                val created = GrpcUpstream(chain, client!!, objectMapper)
                known[chain] = created
                created.start()
                created
            } else {
                current
            }
        }
    }

    fun get(chain: Chain): GrpcUpstream {
        return known[chain]!!
    }
}