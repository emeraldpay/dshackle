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
package io.emeraldpay.dshackle.data

import org.slf4j.LoggerFactory

class DefaultContainer<T>(
    val txId: TxId? = null,
    val blockId: BlockId? = null,
    val height: Long? = null,
    json: ByteArray,
    parsed: T? = null,
) : SourceContainer(json, parsed) {
    companion object {
        private val log = LoggerFactory.getLogger(DefaultContainer::class.java)
    }
}
