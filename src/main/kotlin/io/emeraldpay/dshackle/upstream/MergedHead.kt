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
package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.cache.CachesEnabled
import org.springframework.context.Lifecycle
import reactor.core.Disposable
import reactor.core.publisher.Flux

class MergedHead(
        private val sources: Iterable<Head>
) : AbstractHead(), Lifecycle, CachesEnabled {

    private var subscription: Disposable? = null

    override fun isRunning(): Boolean {
        return subscription != null
    }

    override fun start() {
        subscription = super.follow(Flux.merge(sources.map { it.getFlux() }))
    }

    override fun stop() {
        subscription?.dispose()
    }

    override fun setCaches(caches: Caches) {
        sources.forEach {
            if (it is CachesEnabled) {
                it.setCaches(caches)
            }
        }
    }

}