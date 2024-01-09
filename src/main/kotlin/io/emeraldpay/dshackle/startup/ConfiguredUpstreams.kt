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

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.foundation.ChainOptions
import io.emeraldpay.dshackle.startup.configure.UpstreamCreationData
import io.emeraldpay.dshackle.startup.configure.UpstreamFactory
import io.emeraldpay.dshackle.upstream.CurrentMultistreamHolder
import org.slf4j.LoggerFactory
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.stereotype.Component

@Component
open class ConfiguredUpstreams(
    private val upstreamFactory: UpstreamFactory,
    private val config: UpstreamsConfig,
    private val multistreamHolder: CurrentMultistreamHolder,
) : ApplicationRunner {
    private val log = LoggerFactory.getLogger(ConfiguredUpstreams::class.java)

    override fun run(args: ApplicationArguments) {
        log.debug("Starting upstreams")
        processUpstreams(this.config)
    }

    fun processUpstreams(config: UpstreamsConfig) {
        val defaultOptions = buildDefaultOptions(config)
        config.upstreams.parallelStream().forEach { up ->
            if (!up.isEnabled) {
                log.debug("Upstream ${up.id} is disabled")
                return@forEach
            }
            log.debug("Start upstream ${up.id}")
            if (up.connection !is UpstreamsConfig.GrpcConnection) {
                val chain = Global.chainById(up.chain)
                if (chain == Chain.UNSPECIFIED) {
                    log.error("Chain is unknown: ${up.chain}")
                    return@forEach
                }
                val upstreamCreationData = upstreamFactory.createUpstream(chain.type, up, defaultOptions)
                if (upstreamCreationData != UpstreamCreationData.default()) {
                    val eventType = if (upstreamCreationData.isValid) {
                        UpstreamChangeEvent.ChangeType.ADDED
                    } else {
                        UpstreamChangeEvent.ChangeType.OBSERVED
                    }
                    multistreamHolder.getUpstream(chain)
                        .processUpstreamsEvents(
                            UpstreamChangeEvent(
                                chain,
                                upstreamCreationData.upstream!!,
                                eventType,
                            ),
                        )
                }
            }
        }
    }

    private fun buildDefaultOptions(config: UpstreamsConfig): HashMap<Chain, ChainOptions.PartialOptions> {
        val defaultOptions = HashMap<Chain, ChainOptions.PartialOptions>()
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
        return defaultOptions
    }
}
