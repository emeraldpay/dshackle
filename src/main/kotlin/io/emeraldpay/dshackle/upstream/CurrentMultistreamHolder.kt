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
package io.emeraldpay.dshackle.upstream

import io.emeraldpay.grpc.Chain
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import javax.annotation.PreDestroy

@Component
open class CurrentMultistreamHolder(
    multistreams: List<Multistream>
) : MultistreamHolder {

    private val log = LoggerFactory.getLogger(CurrentMultistreamHolder::class.java)

    private val chainMapping = multistreams.associateBy { it.chain }

    override fun getUpstream(chain: Chain): Multistream? {
        return chainMapping[chain]
    }

    override fun getAvailable(): List<Chain> {
        return chainMapping.values.asSequence()
            .filter { it.haveUpstreams() }
            .map { it.chain }
            .toList()
    }

    override fun isAvailable(chain: Chain): Boolean {
        return chainMapping.getValue(chain).isAvailable()
    }

    @PreDestroy
    fun shutdown() {
        log.info("Closing upstream connections...")
        chainMapping.values.forEach {
            it.stop()
        }
    }
}
