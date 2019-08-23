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

class FilteringApiIterator(
        private val upstreams: List<Upstream>,
        private var pos: Int,
        private val matcher: Selector.Matcher,
        private val repeatLimit: Int = 3
): Iterator<DirectEthereumApi> {

    private var nextUpstream: Upstream? = null
    private var consumed = 0

    private fun nextInternal(): Boolean {
        if (nextUpstream != null) {
            return true
        }
        while (nextUpstream == null) {
            consumed++
            if (consumed > upstreams.size * repeatLimit) {
                return false
            }
            val upstream = upstreams[pos++ % upstreams.size]
            if (upstream.isAvailable(matcher)) {
                nextUpstream = upstream
            }
        }
        return nextUpstream != null
    }

    override fun hasNext(): Boolean {
        return nextInternal()
    }

    override fun next(): DirectEthereumApi {
        if (nextInternal()) {
            val curr = nextUpstream!!
            nextUpstream = null
            return curr.getApi(matcher)
        }
        throw IllegalStateException("No upstream API available")
    }
}