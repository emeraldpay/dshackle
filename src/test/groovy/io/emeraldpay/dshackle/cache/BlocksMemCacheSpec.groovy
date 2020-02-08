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
package io.emeraldpay.dshackle.cache

import io.infinitape.etherjar.domain.BlockHash
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.json.TransactionRefJson
import spock.lang.Specification

class BlocksMemCacheSpec extends Specification {

    String hash1 = "0xd3f34def3c56ba4e701540d15edaff9acd2a1c968a7ff83b3300ab5dfd5f6aab"
    String hash2 = "0x4aabdaff9acd2f30d15e00ab5dfd5f6c56ba4ea1c968a7ff8d3f34de70153b33"
    String hash3 = "0x40d15edaff9acdabd2a1c96fd5f683b3300aad34e7015f34def3c56ba8a7ffb5"
    String hash4 = "0xa4e7a75dfd5f6a83b3304dc56bfa0abfd3fef01540d15edafc9683f9acd2a13b"

    def "Add and read"() {
        setup:
        def cache = new BlocksMemCache()
        def block = new BlockJson<TransactionRefJson>()
        block.number = 100
        block.hash = BlockHash.from(hash1)

        when:
        cache.add(block)
        def act = cache.read(BlockHash.from(hash1)).block()
        then:
        act == block
    }

    def "Keeps only configured amount"() {
        setup:
        def cache = new BlocksMemCache(3)
        [hash1]

        when:
        [hash1, hash2, hash3, hash4].eachWithIndex{ String hash, int i ->
            def block = new BlockJson<TransactionRefJson>()
            block.number = 100 + i
            block.hash = BlockHash.from(hash)
            cache.add(block)
        }

        def act1 = cache.read(BlockHash.from(hash1)).block()
        def act2 = cache.read(BlockHash.from(hash2)).block()
        def act3 = cache.read(BlockHash.from(hash3)).block()
        def act4 = cache.read(BlockHash.from(hash4)).block()
        then:
        act2.hash.toHex() == hash2
        act3.hash.toHex() == hash3
        act4.hash.toHex() == hash4
        act1 == null
    }

}
