/**
 * Copyright (c) 2021 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.upstream.bitcoin

import io.emeraldpay.dshackle.upstream.ChainFees
import io.emeraldpay.dshackle.upstream.Head
import reactor.core.publisher.Mono
import spock.lang.Specification

import java.time.Duration

class BitcoinFeesSpec extends Specification {

    def block3 = [
            hash: "3300000000033327aa200b1cecaad478d2b00432346c3f1f3986da1afd33e506",
            tx  : [
                    "d5a67d8f99ad05ece282bc8714da55cbd2266d9d7bae98fd5dda3b55d1696fb8",
                    "e17eca6b595da9c1c7b883fe505184d7ff586315d5b8a0c476198f0d49a75ad3",
                    "1735dfef80bcfdc443943dcbc8d26a4133da87c5c81d9e2a1e0a0e5f59d1ef96",
                    "c83c99e50946a6d30bc344a24401ad0ff0598425d73a5f74d6545e15ae588321",
                    "0d4f85f840f09f317d0b202725acee868b2e969078bb6a6b89fc3e0bc1216c6a",
                    "4d56b1a0992a3fa3132083ce29e203165ef4768f028d98a738da79dd26ff91e2",
            ]
    ]
    def block2 = [
            hash: "2200000000022227aa200b1cecaad478d2b00432346c3f1f3986da1afd33e506",
            tx  : [
                    "87cc7d37851af41da60a5d14e40983d0a6750ffcdf1f2ae1291bfc1e712f0712",
                    "3bde80635c62e81ba86d68a1c78ac47b1a02d8ef334fdf6a7b6e8acc0a475f20",
                    "2f4f413c07ac072918e12fcac4afd8d30a76dfe49ac68d46d582c3a130772734",
                    "f29ecfa5b692cfc46b1eae29416a35782657f0bde2ae44acde882e477e8624d8",
            ]
    ]

    def tx4D5 = [
            hash : "4d56b1a0992a3fa3132083ce29e203165ef4768f028d98a738da79dd26ff91e2",
            size : 223,
            vsize: 142,
            vin  : [
                    [txid: "3bde80635c62e81ba86d68a1c78ac47b1a02d8ef334fdf6a7b6e8acc0a475f20", vout: 1]
            ],
            vout : [
                    ["value": 0.00029163, "n": 0],
                    ["value": 0.00144221, "n": 1]
            ]
    ]
    def txF29 = [
            hash : "f29ecfa5b692cfc46b1eae29416a35782657f0bde2ae44acde882e477e8624d8",
            size : 2900,
            vsize: 1366,
            vin  : [
                    [txid: "37e19fad6c3e5fafc431ee731428452dc01f56b0bc4d9fe876dcee58384934ec", "vout": 0],
                    [txid: "3da079b10451ba1cde42d9a0f485e79151e5d4216930561227be5471921af17f", "vout": 0]
            ],
            vout : [
                    ["value": 1.20100000, "n": 0],
                    ["value": 0.00210984, "n": 1]
            ]
    ]

    def tx3BD = [
            hash: "3bde80635c62e81ba86d68a1c78ac47b1a02d8ef334fdf6a7b6e8acc0a475f20",
            size: 292,
            vin : [
            ],
            vout: [
                    ["value": 0.00400000, "n": 0],
                    ["value": 0.00200000, "n": 1],
            ]
    ]
    def tx37E = [
            hash : "37e19fad6c3e5fafc431ee731428452dc01f56b0bc4d9fe876dcee58384934ec",
            size : 292,
            vsize: 200,
            vin  : [
            ],
            vout : [
                    ["value": 1.00000000, "n": 0],
            ]
    ]
    def tx3DA = [
            hash : "3da079b10451ba1cde42d9a0f485e79151e5d4216930561227be5471921af17f",
            size : 297,
            vsize: 201,
            vin  : [
            ],
            vout : [
                    ["value": 0.21000000, "n": 0],
            ]
    ]


    def "fetch tx amount"() {
        setup:
        def reader = Mock(BitcoinEgressReader) {
            1 * it.getTx("4d56b1a0992a3fa3132083ce29e203165ef4768f028d98a738da79dd26ff91e2") >> Mono.just(tx4D5)
        }
        def fees = new BitcoinFees(Stub(BitcoinMultistream), reader, 3)

        when:
        def amount = fees.getTxOutAmount("4d56b1a0992a3fa3132083ce29e203165ef4768f028d98a738da79dd26ff91e2", 0)
                .block(Duration.ofSeconds(1))
        then:
        amount == 29163
    }

    def "extract amounts"() {
        setup:
        def fees = new BitcoinFees(Stub(BitcoinMultistream), Stub(BitcoinEgressReader), 3)

        when:
        def act = fees.extractVOuts(txF29)
        then:
        act == [120100000L, 210984L]
    }

    def "extract vsize when available"() {
        setup:
        def fees = new BitcoinFees(Stub(BitcoinMultistream), Stub(BitcoinEgressReader), 3)

        when:
        def act = fees.extractSize(tx4D5)
        then:
        act == 142
    }

    def "extract size when vsize unavailable"() {
        setup:
        def fees = new BitcoinFees(Stub(BitcoinMultistream), Stub(BitcoinEgressReader), 3)

        when:
        def act = fees.extractSize(tx3BD)
        then:
        act == 292
    }

    def "calculates fee"() {
        setup:
        def reader = Mock(BitcoinEgressReader) {
            1 * it.getTx("3bde80635c62e81ba86d68a1c78ac47b1a02d8ef334fdf6a7b6e8acc0a475f20") >> Mono.just(tx3BD)
        }
        def fees = new BitcoinFees(Stub(BitcoinMultistream), reader, 3)
        when:
        def act = fees.calculateFee(tx4D5).block(Duration.ofSeconds(1))
        then:
        act == 200000 - (29163 + 144221)
    }

    def "calculates fee with multiple inputs"() {
        setup:
        def reader = Mock(BitcoinEgressReader) {
            1 * it.getTx("37e19fad6c3e5fafc431ee731428452dc01f56b0bc4d9fe876dcee58384934ec") >> Mono.just(tx37E)
            1 * it.getTx("3da079b10451ba1cde42d9a0f485e79151e5d4216930561227be5471921af17f") >> Mono.just(tx3DA)
        }
        def fees = new BitcoinFees(Stub(BitcoinMultistream), reader, 3)
        when:
        def act = fees.calculateFee(txF29).block(Duration.ofSeconds(1))
        then:
        act == (100000000 + 21000000) - (120100000 + 210984)
    }

    def "get average bottom fee"() {
        setup:
        def ups = Mock(BitcoinMultistream) {
            _ * getHead() >> Mock(Head) {
                _ * getCurrentHeight() >> 100
            }
        }
        def reader = Mock(BitcoinEgressReader) {
            1 * it.getBlock(100) >> Mono.just(block3)
            1 * it.getBlock(99) >> Mono.just(block2)
            // last txes on those blocks
            1 * it.getTx("4d56b1a0992a3fa3132083ce29e203165ef4768f028d98a738da79dd26ff91e2") >> Mono.just(tx4D5)
            1 * it.getTx("f29ecfa5b692cfc46b1eae29416a35782657f0bde2ae44acde882e477e8624d8") >> Mono.just(txF29)
            // their inputs
            1 * it.getTx("3bde80635c62e81ba86d68a1c78ac47b1a02d8ef334fdf6a7b6e8acc0a475f20") >> Mono.just(tx3BD)
            1 * it.getTx("37e19fad6c3e5fafc431ee731428452dc01f56b0bc4d9fe876dcee58384934ec") >> Mono.just(tx37E)
            1 * it.getTx("3da079b10451ba1cde42d9a0f485e79151e5d4216930561227be5471921af17f") >> Mono.just(tx3DA)
        }
        def fees = new BitcoinFees(ups, reader, 3)

        when:
        def act = fees.estimate(ChainFees.Mode.AVG_LAST, 2).block(Duration.ofSeconds(1))

        then:
        act.hasBitcoinStd()
        // first tx fee: 187.43661971830985915493
        // second tx fee: 504.40409956076134699854
        // average is 345
        // but if we calculate original fees per KB it's 354222
        act.bitcoinStd.satPerKb == 354222
    }

}
