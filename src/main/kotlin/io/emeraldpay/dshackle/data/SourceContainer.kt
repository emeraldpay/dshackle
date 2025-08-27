/**
 * Copyright (c) 2020 EmeraldPay, Inc
 * Copyright (c) 2020 ETCDEV GmbH
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
package io.emeraldpay.dshackle.data

abstract class SourceContainer(
    val json: ByteArray?,
    private val parsed: Any?,
) {
    @Suppress("UNCHECKED_CAST")
    fun <T> getParsed(clazz: Class<T>): T? {
        if (parsed == null) {
            return null
        }
        if (clazz.isAssignableFrom(parsed.javaClass)) {
            return parsed as T
        }
        throw ClassCastException("Cannot cast ${parsed.javaClass} to $clazz")
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is SourceContainer) return false

        if (json != null) {
            if (other.json == null) return false
            if (!json.contentEquals(other.json)) return false
        } else if (other.json != null) {
            return false
        }

        return true
    }

    override fun hashCode(): Int = json?.contentHashCode() ?: 0
}
