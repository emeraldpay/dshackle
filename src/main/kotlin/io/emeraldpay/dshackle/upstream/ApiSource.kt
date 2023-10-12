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

import org.reactivestreams.Publisher

interface ApiSource : Publisher<Upstream> {

    /**
     * Total size of the available upstreams list, including the upstreams that may be not available (not matched) at the moment.
     * Because otherwise a matcher for "fresh upstreams" Matcher it may limit to the size of one, and a Quorum processor
     * may not work properly thinking that the whole ApiSource is based on a single upstream.
     */
    val size: Int

    fun resolve()

    /**
     * Must be called before actual use, it spins off control flow of the API Source
     */
    fun request(tries: Int)

    /**
     * Exclude upstream from the current list of upstreams
     */
    fun exclude(upstream: Upstream)
}
