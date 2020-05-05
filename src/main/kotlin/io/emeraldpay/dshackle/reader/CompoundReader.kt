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
package io.emeraldpay.dshackle.reader

import io.emeraldpay.dshackle.Defaults
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Duration

/**
 * Composition of multiple readers. Reader returns first value returned by any of the source readers.
 */
class CompoundReader<K, D>(
        private vararg val readers: Reader<K, D>
): Reader<K, D> {

    companion object {
        private val log = LoggerFactory.getLogger(CompoundReader::class.java)
    }

    override fun read(key: K): Mono<D> {
        if (readers.isEmpty()) {
            return Mono.empty()
        }
        return Flux.fromIterable(readers.asIterable())
                .flatMap { rdr ->
                    rdr.read(key)
                            .timeout(Defaults.timeoutInternal, Mono.empty())
                            .doOnError { t -> log.warn("Failed to read from $rdr", t) }
                            .onErrorResume { Mono.empty() }
                }.next()
    }

}