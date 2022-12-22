/**
 * Copyright (c) 2022 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.commons

import io.emeraldpay.etherjar.domain.TransactionId
import io.emeraldpay.etherjar.hex.HexDataComparator
import io.emeraldpay.etherjar.tx.Transaction
import spock.lang.Specification

import java.time.Duration

class ExpiringSetSpec extends Specification {

    def "Add and check item"() {
        setup:
        def set = new ExpiringSet(Duration.ofSeconds(60), new HexDataComparator() as Comparator<TransactionId>, 100)

        when:
        def firstAdded = set.add(TransactionId.from("0xc008b506367e1f96fcfdf6b683e84601434f8b655334bc61dae7970f0fb7d02c"))
        def firstExists = set.contains(TransactionId.from("0xc008b506367e1f96fcfdf6b683e84601434f8b655334bc61dae7970f0fb7d02c"))
        def secondAdded = set.add(TransactionId.from("0x424e36776777ddd2877df0f9d278c37077d1e00afa84defcfb6b367880d8eb6d"))
        def secondExists = set.contains(TransactionId.from("0x424e36776777ddd2877df0f9d278c37077d1e00afa84defcfb6b367880d8eb6d"))
        def thirdExists = set.contains(TransactionId.from("0xe218f09c3060099cb7302304e2a29ed7a8693bd5dfe3a55cce96fabce27012e0"))

        then:
        firstAdded
        secondAdded
        firstExists
        secondExists
        !thirdExists

        set.size == 2
    }

    def "Doesn't grow after limit"() {
        setup:
        def set = new ExpiringSet(Duration.ofSeconds(60), new HexDataComparator() as Comparator<TransactionId>, 3)

        when:
        set.add(TransactionId.from("0xc008b506367e1f96fcfdf6b683e84601434f8b655334bc61dae7970f0fb7d02c"))
        set.add(TransactionId.from("0x424e36776777ddd2877df0f9d278c37077d1e00afa84defcfb6b367880d8eb6d"))
        set.add(TransactionId.from("0xe218f09c3060099cb7302304e2a29ed7a8693bd5dfe3a55cce96fabce27012e0"))
        set.add(TransactionId.from("0xd3082daa344a64369c8aace137f22beb5085351bf111859202bac66b70b28bdd"))

        then:
        set.size == 3

        when:
        def firstExists = set.contains(TransactionId.from("0xc008b506367e1f96fcfdf6b683e84601434f8b655334bc61dae7970f0fb7d02c"))
        def secondExists = set.contains(TransactionId.from("0x424e36776777ddd2877df0f9d278c37077d1e00afa84defcfb6b367880d8eb6d"))
        def thirdExists = set.contains(TransactionId.from("0xe218f09c3060099cb7302304e2a29ed7a8693bd5dfe3a55cce96fabce27012e0"))

        then:
        !firstExists
        secondExists
        thirdExists
    }

    def "Remove expired"() {
        setup:
        def set = new ExpiringSet(Duration.ofMillis(100), new HexDataComparator() as Comparator<TransactionId>, 100)

        when:
        set.add(TransactionId.from("0x1118b506367e1f96fcfdf6b683e84601434f8b655334bc61dae7970f0fb7d02c"))
        set.add(TransactionId.from("0x222e36776777ddd2877df0f9d278c37077d1e00afa84defcfb6b367880d8eb6d"))

        then:
        set.size == 2

        when:
        Thread.sleep(60)
        set.add(TransactionId.from("0x3338f09c3060099cb7302304e2a29ed7a8693bd5dfe3a55cce96fabce27012e0"))
        set.add(TransactionId.from("0x44482daa344a64369c8aace137f22beb5085351bf111859202bac66b70b28bdd"))

        then:
        set.size == 4

        when:
        Thread.sleep(60)
        set.add(TransactionId.from("0x55531d466acf4ef72f7e0fbc60a5c2c9d2845b90a3d27b6d7581575cb119cac9"))
        set.add(TransactionId.from("0x6665e1f32cb21aee5f27d804cfc65781d5c140b002776bc073f9405479e8b1e5"))

        then:
        set.size == 4
        set.contains(TransactionId.from("0x3338f09c3060099cb7302304e2a29ed7a8693bd5dfe3a55cce96fabce27012e0"))
        set.contains(TransactionId.from("0x44482daa344a64369c8aace137f22beb5085351bf111859202bac66b70b28bdd"))
        set.contains(TransactionId.from("0x55531d466acf4ef72f7e0fbc60a5c2c9d2845b90a3d27b6d7581575cb119cac9"))
        set.contains(TransactionId.from("0x6665e1f32cb21aee5f27d804cfc65781d5c140b002776bc073f9405479e8b1e5"))
    }

}
