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

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.foundation.ChainOptions
import io.emeraldpay.dshackle.reader.ChainReader
import io.emeraldpay.dshackle.startup.UpstreamChangeEvent
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.finalization.FinalizationData
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundData
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundType
import reactor.core.publisher.Flux

interface Upstream : Lifecycle {
    fun isAvailable(): Boolean
    fun getStatus(): UpstreamAvailability
    fun observeStatus(): Flux<UpstreamAvailability>
    fun observeState(): Flux<UpstreamChangeEvent>
    fun getHead(): Head

    /**
     * Get an actual reader that access the current upstream
     */
    fun getIngressReader(): ChainReader

    fun getOptions(): ChainOptions.Options
    fun getRole(): UpstreamsConfig.UpstreamRole
    fun setLag(lag: Long)
    fun getLag(): Long?
    fun getLabels(): Collection<UpstreamsConfig.Labels>
    fun getMethods(): CallMethods
    fun updateMethods(m: CallMethods)
    fun getId(): String
    fun getCapabilities(): Set<Capability>
    fun isGrpc(): Boolean
    fun getLowerBounds(): Collection<LowerBoundData>
    fun getLowerBound(lowerBoundType: LowerBoundType): LowerBoundData?
    fun getFinalizations(): Collection<FinalizationData>
    fun addFinalization(finalization: FinalizationData, upstreamId: String)
    fun getUpstreamSettingsData(): UpstreamSettingsData?
    fun updateLowerBound(lowerBound: Long, type: LowerBoundType)

    fun getChain(): Chain

    fun <T : Upstream> cast(selfType: Class<T>): T

    fun nodeId(): Short

    data class UpstreamSettingsData(
        val nodeId: Short,
        val id: String,
        val nodeVersion: String,
    ) {
        constructor(id: String) : this(0, id, UNKNOWN_CLIENT_VERSION)
    }
}
