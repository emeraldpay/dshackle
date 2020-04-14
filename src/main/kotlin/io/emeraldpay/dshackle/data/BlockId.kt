/**
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

import io.infinitape.etherjar.rpc.json.BlockJson

class BlockId(
        value: ByteArray
) : HashId(value) {

    companion object {
        @JvmStatic
        fun from(hash: io.infinitape.etherjar.domain.BlockHash): BlockId {
            return BlockId(hash.bytes)
        }

        @JvmStatic
        fun from(block: BlockJson<*>): BlockId {
            return from(block.hash)
        }

        @JvmStatic
        fun from(id: String): BlockId {
            return from(io.infinitape.etherjar.domain.BlockHash.from(id))
        }
    }


}