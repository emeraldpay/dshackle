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

open class HashId(
    val value: ByteArray,
) {
    companion object {
        val HEX_DIGITS = "0123456789abcdef".toCharArray()
    }

    override fun toString(): String = toHex()

    fun toHex(): String {
        val hex = CharArray(value.size * 2)
        var i = 0
        var j = 0
        while (i < value.size) {
            hex[j++] = HEX_DIGITS[0xF0 and value[i].toInt() ushr 4]
            hex[j++] = HEX_DIGITS[0x0F and value[i].toInt()]
            i++
        }
        return String(hex)
    }

    fun toHexWithPrefix(): String = "0x" + toHex()

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is HashId) return false

        if (!value.contentEquals(other.value)) return false

        return true
    }

    override fun hashCode(): Int = value.contentHashCode()
}
