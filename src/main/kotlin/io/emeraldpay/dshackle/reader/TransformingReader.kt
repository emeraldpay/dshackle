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
 * Reader wrapper that transforms output of the reader to a different format
 */
class TransformingReader<K, D0, D>(
    /**
     * Actual reader
     */
    private val reader: Reader<K, D0>,
    /**
     * Result transformation
     */
    private val transformer: Function<in D0, out D>
) : Reader<K, D> {

    override fun read(key: K): Mono<D> {
        return reader.read(key).map(transformer)
    }
}
