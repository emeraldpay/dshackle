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

import io.emeraldpay.etherjar.domain.BlockHash
import spock.lang.Specification

class BlockIdSpec extends Specification {

    def "Create from string"() {
        expect:
        BlockId.from(source).toHex() == exp
        where:
        exp                                                                | source
        "a0e65cbc1b52a8ca60562112c6060552d882f16f34a9dba2ccdc05c0a6a27100" | "a0e65cbc1b52a8ca60562112c6060552d882f16f34a9dba2ccdc05c0a6a27100"
        "a0e65cbc1b52a8ca60562112c6060552d882f16f34a9dba2ccdc05c0a6a27100" | "A0E65CBC1B52A8CA60562112C6060552D882F16F34A9DBA2CCDC05C0A6A27100"
        "a0e65cbc1b52a8ca60562112c6060552d882f16f34a9dba2ccdc05c0a6a27100" | "0xa0e65cbc1b52a8ca60562112c6060552d882f16f34a9dba2ccdc05c0a6a27100"
        "a0e65cbc1b52a8ca60562112c6060552d882f16f34a9dba2ccdc05c0a6a27100" | "0xA0E65CBC1B52A8CA60562112C6060552D882F16F34A9DBA2CCDC05C0A6A27100"
        "00e65cbc1b52a8ca60562112c6060552d882f16f34a9dba2ccdc05c0a6a27100" | "00e65cbc1b52a8ca60562112c6060552d882f16f34a9dba2ccdc05c0a6a27100"
        "00e65cbc1b52a8ca60562112c6060552d882f16f34a9dba2ccdc05c0a6a27100" | "0x00e65cbc1b52a8ca60562112c6060552d882f16f34a9dba2ccdc05c0a6a27100"
    }

    def "Create from block hash"() {
        expect:
        BlockId.from(BlockHash.from(source)).toHex() == exp
        where:
        exp                                                                | source
        "a0e65cbc1b52a8ca60562112c6060552d882f16f34a9dba2ccdc05c0a6a27100" | "0xa0e65cbc1b52a8ca60562112c6060552d882f16f34a9dba2ccdc05c0a6a27100"
        "00e65cbc1b52a8ca60562112c6060552d882f16f34a9dba2ccdc05c0a6a27100" | "0x00e65cbc1b52a8ca60562112c6060552d882f16f34a9dba2ccdc05c0a6a27100"
    }

    def "Same hash makes equal ids"() {
        expect:
        BlockId.from("01") == BlockId.from("01")
    }

    def "Different hashes makes non equal ids"() {
        expect:
        BlockId.from("01") != BlockId.from("02")
    }

}
