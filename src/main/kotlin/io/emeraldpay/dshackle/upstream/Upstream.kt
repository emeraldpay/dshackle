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

import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.reader.JsonRpcReader
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import reactor.core.publisher.Flux

interface Upstream {

    fun isAvailable(): Boolean
    fun getStatus(): UpstreamAvailability
    fun observeStatus(): Flux<UpstreamAvailability>
    fun getHead(): Head

    /**
     * Get an actual reader that access the current upstream
     */
    fun getIngressReader(): JsonRpcReader

    fun getOptions(): UpstreamsConfig.Options
    fun getRole(): UpstreamsConfig.UpstreamRole
    fun setLag(lag: Long)
    fun getLag(): Long
    fun getLabels(): Collection<UpstreamsConfig.Labels>
    fun getMethods(): CallMethods
    fun getId(): String
    fun getCapabilities(): Set<Capability>
    fun isGrpc(): Boolean

    fun <T : Upstream> cast(selfType: Class<T>): T
}
