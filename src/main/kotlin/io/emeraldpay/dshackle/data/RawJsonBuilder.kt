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

import org.slf4j.LoggerFactory
import java.io.ByteArrayOutputStream

class RawJsonBuilder {

    companion object {
        private val log = LoggerFactory.getLogger(RawJsonBuilder::class.java)

        private val START = "{\"jsonrpc\":\"2.0\"".toByteArray()
        private val ID_START = "\"id\":".toByteArray()
        private val RESULT_START = "\"result\":".toByteArray()
        private val COMMA = ",".toByteArray()
        private val END = "}".toByteArray()
    }

    fun write(id: Int, data: ByteArray): ByteArray {
        val buf = ByteArrayOutputStream(data.size + 100)
        buf.write(START)
        buf.write(COMMA)
        buf.write(ID_START)
        buf.write(id.toString().toByteArray());
        buf.write(COMMA)
        buf.write(RESULT_START)
        buf.write(data)
        buf.write(END)

        return buf.toByteArray()
    }


}