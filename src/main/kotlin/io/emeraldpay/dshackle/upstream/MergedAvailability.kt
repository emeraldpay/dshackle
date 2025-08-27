/**
 * Copyright (c) 2022 EmeraldPay, Inc
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
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import java.util.function.BiFunction

class MergedAvailability(
    private val source1: Publisher<UpstreamAvailability>,
    private val source2: Publisher<UpstreamAvailability>,
) {
    companion object {
        private val log = LoggerFactory.getLogger(MergedAvailability::class.java)
    }

    fun produce(): Flux<UpstreamAvailability> {
        val selectWorst =
            BiFunction<UpstreamAvailability, UpstreamAvailability, UpstreamAvailability> { a, b ->
                // the worst availability represents the final state. I.e. if any checks fails provide with it.
                if (a.isBetterTo(b)) b else a
            }

        return Flux
            .combineLatest(source1, source2, selectWorst)
            .distinctUntilChanged()
    }
}
