/**
 * Copyright (c) 2020 EmeraldPay, Inc
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

import reactor.core.publisher.Mono
import java.util.function.Function

/**
 * Reader wrapper that maps the input key from ne value to another (ex. convert from Long to String)
 */
class RekeyingReader<K, K1, D>(
        /**
         * Mapping between original Key and Key supported by the reader
         */
        private val rekey: Function<K, K1>,
        /**
         * Actual reader
         */
        private val reader: Reader<K1, D>
) : Reader<K, D> {

    override fun read(key: K): Mono<D> {
        return Mono.just(key)
                .map(rekey)
                .flatMap {
                    reader.read(it)
                }
    }

}