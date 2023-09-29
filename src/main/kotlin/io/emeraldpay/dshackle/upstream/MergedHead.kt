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

import com.google.common.annotations.VisibleForTesting
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.cache.CachesEnabled
import io.emeraldpay.dshackle.upstream.forkchoice.ForkChoice
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.scheduler.Scheduler

class MergedHead @JvmOverloads constructor(
    private val sources: Iterable<Head>,
    forkChoice: ForkChoice,
    headScheduler: Scheduler,
    private val label: String = "",
) : AbstractHead(forkChoice, headScheduler, upstreamId = label), Lifecycle, CachesEnabled {

    private var subscription: Disposable? = null

    override fun isRunning(): Boolean {
        return subscription != null
    }

    override fun start() {
        super.start()
        sources.forEach { head ->
            if (head is Lifecycle && !head.isRunning()) {
                head.start()
            }
        }
        subscription?.dispose()
        subscription = super.follow(
            Flux.merge(sources.map { it.getFlux() }).doOnNext {
                log.debug("New MERGED $label head $it")
            },
        )
    }

    override fun stop() {
        super.stop()
        sources.forEach { head ->
            if (head is Lifecycle && head.isRunning()) {
                head.stop()
            }
        }
        subscription?.dispose()
        subscription = null
    }

    override fun setCaches(caches: Caches) {
        sources.forEach {
            if (it is CachesEnabled) {
                it.setCaches(caches)
            }
        }
    }

    @VisibleForTesting
    private fun getSources() =
        sources
}
