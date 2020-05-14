/**
 * Copyright (c) 2020 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.upstream.bitcoin

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import reactor.core.Disposable
import reactor.core.publisher.Mono
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock

open class CachingMempoolData(
        private val upstreams: BitcoinChainUpstreams,
        private val head: Head,
        private val objectMapper: ObjectMapper
) : Lifecycle {

    companion object {
        private val log = LoggerFactory.getLogger(CachingMempoolData::class.java)
        private val TTL = Duration.ofSeconds(15)
    }

    private val current = AtomicReference<Container>(Container.empty())
    private val updateLock = ReentrantLock()
    private var headListener: Disposable? = null

    open fun get(): Mono<List<String>> {
        val value = current.get()
        return if (value.since < Instant.now().minus(TTL)) {
            updateLock.lock()
            fetchFromUpstream()
                    .timeout(Duration.ofSeconds(3), Mono.empty())
                    .doOnNext {
                        current.set(Container(Instant.now(), it))
                    }.doFinally {
                        updateLock.unlock()
                    }
        } else {
            Mono.just(value.value)
        }
    }

    fun fetchFromUpstream(): Mono<List<String>> {
        return upstreams.getDirectApi(Selector.empty).flatMap { api ->
            api.read(JsonRpcRequest("getrawmempool", emptyList()))
                    .flatMap(JsonRpcResponse::requireResult)
                    .map { objectMapper.readValue(it, List::class.java) as List<String> }
        }
    }

    class Container(val since: Instant, val value: List<String>) {
        companion object {
            fun empty(): Container {
                return Container(Instant.MIN, emptyList())
            }
        }
    }

    override fun isRunning(): Boolean {
        return headListener != null
    }

    override fun start() {
        headListener?.dispose()
        headListener = head.getFlux().doOnNext {
            current.set(Container.empty())
        }.subscribe()
    }

    override fun stop() {
        headListener?.dispose()
        headListener = null
    }

}