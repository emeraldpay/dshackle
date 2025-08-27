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
package io.emeraldpay.dshackle.upstream.calls

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.quorum.AlwaysQuorum
import io.emeraldpay.dshackle.quorum.CallQuorum
import io.emeraldpay.dshackle.quorum.NonEmptyQuorum
import io.emeraldpay.dshackle.quorum.NotLaggingQuorum
import org.apache.commons.collections4.Factory
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.Collections

/**
 * Wrapper on top of another configuration, that may disable or enable additional methods on top of it.
 * If a new method enabled, then its Quorum will be [AlwaysQuorum]. For other methods it delegates all to the provided
 * parent config.
 */
class ManagedCallMethods(
    private val delegate: CallMethods,
    private val enabled: Set<String>,
    private val disabled: Set<String>,
) : CallMethods {
    companion object {
        private val log = LoggerFactory.getLogger(ManagedCallMethods::class.java)
        private val defaultQuorum: Factory<CallQuorum> =
            Factory<CallQuorum> {
                AlwaysQuorum()
            }
    }

    private val delegated = delegate.getSupportedMethods().sorted()
    private val allAllowed: Set<String> =
        Collections.unmodifiableSet(
            enabled + delegated - disabled,
        )
    private val quorum: MutableMap<String, Factory<CallQuorum>> = HashMap()
    private val staticResponse: MutableMap<String, String> = HashMap()
    private val redefined = delegated.filter(enabled::contains).sorted()

    init {
        enabled.forEach { m ->
            quorum[m] = defaultQuorum
        }
    }

    fun setQuorum(
        method: String,
        quorumId: String,
    ) {
        val quorum =
            when (quorumId) {
                "always" -> Factory<CallQuorum> { AlwaysQuorum() }
                "no-lag", "not-lagging", "no_lag", "not_lagging" -> Factory<CallQuorum> { NotLaggingQuorum(0) }
                "not-empty", "not_empty", "non-empty", "non_empty" -> Factory<CallQuorum> { NonEmptyQuorum() }
                else -> {
                    log.warn("Unknown quorum: $quorumId for custom method $method")
                    return
                }
            }
        this.quorum[method] = quorum
    }

    fun setStaticResponse(
        method: String,
        response: String,
    ) {
        this.staticResponse[method] = response
    }

    override fun createQuorumFor(method: String): CallQuorum =
        when {
            isDelegated(method) && !isRedefined(method) -> delegate.createQuorumFor(method)
            enabled.contains(method) -> quorum[method]?.create() ?: defaultQuorum.create()
            else -> {
                log.warn("Getting quorum for unknown method")
                defaultQuorum.create()
            }
        }

    private fun isDelegated(method: String): Boolean = Collections.binarySearch(delegated, method) >= 0

    private fun isRedefined(method: String): Boolean = Collections.binarySearch(redefined, method) >= 0

    override fun isCallable(method: String): Boolean = allAllowed.contains(method)

    override fun getSupportedMethods(): Set<String> = allAllowed

    override fun isHardcoded(method: String): Boolean = this.staticResponse.containsKey(method) || delegate.isHardcoded(method)

    override fun executeHardcoded(method: String): ByteArray {
        if (this.staticResponse.containsKey(method)) {
            var json: String = this.staticResponse[method].orEmpty()
            // Check if it's valid JSON
            val mapper = ObjectMapper()
            try {
                mapper.readTree(json)
            } catch (e: IOException) {
                // Encode and default to string
                json = mapper.writeValueAsString(json)
            }
            return json.toByteArray()
        }
        return delegate.executeHardcoded(method)
    }
}
