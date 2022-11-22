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
package io.emeraldpay.dshackle.rpc

import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.MultistreamHolder
import io.emeraldpay.dshackle.upstream.bitcoin.BitcoinMultistream
import io.emeraldpay.dshackle.upstream.bitcoin.BitcoinReader
import io.emeraldpay.dshackle.upstream.bitcoin.CachingMempoolData
import io.emeraldpay.dshackle.Chain
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Duration
import java.time.Instant

class TrackBitcoinTxSpec extends Specification {

    def "loadMempool() returns not found when not found"() {
        setup:
        TrackBitcoinTx track = new TrackBitcoinTx(Stub(MultistreamHolder))

        CachingMempoolData mempoolAccess = Mock(CachingMempoolData) {
            1 * get() >> Mono.just([
                    "69cd44d7c641db82e69824523c7ac0c5c1e5628f025474529cf5ffe64527efc9",
                    "d296c6d47335a7f283574b06f1d6303b30ac75631e081ab128346a549ad93350"
            ])
        }
        BitcoinMultistream upstream = Mock(BitcoinMultistream) {
            _ * getReader() >> Mock(BitcoinReader) {
                _ * getMempool() >> mempoolAccess
            }
        }
        when:
        def act = track.loadMempool(upstream, "65ce58db064bd105b14dc76a0bce0df14653cf5263d22d17e78864cf272ee367")

        then:
        StepVerifier.create(act)
                .expectNextMatches {
                    it.found == false && it.mined == false && it.blockHash == null
                }
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "loadMempool() returns ok when found"() {
        setup:
        TrackBitcoinTx track = new TrackBitcoinTx(Stub(MultistreamHolder))
        CachingMempoolData mempoolAccess = Mock(CachingMempoolData) {
            1 * get() >> Mono.just([
                    "69cd44d7c641db82e69824523c7ac0c5c1e5628f025474529cf5ffe64527efc9",
                    "d296c6d47335a7f283574b06f1d6303b30ac75631e081ab128346a549ad93350"
            ])
        }
        BitcoinMultistream upstream = Mock(BitcoinMultistream) {
            _ * getReader() >> Mock(BitcoinReader) {
                _ * getMempool() >> mempoolAccess
            }
        }
        when:
        def act = track.loadMempool(upstream, "69cd44d7c641db82e69824523c7ac0c5c1e5628f025474529cf5ffe64527efc9")

        then:
        StepVerifier.create(act)
                .expectNextMatches {
                    it.found == true && it.mined == false && it.blockHash == null
                }
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "loadExiting() returns not found if not mined"() {
        setup:
        TrackBitcoinTx track = new TrackBitcoinTx(Stub(MultistreamHolder))
        def txid = "69cd44d7c641db82e69824523c7ac0c5c1e5628f025474529cf5ffe64527efc9"
        BitcoinMultistream upstream = Mock(BitcoinMultistream) {
            _ * getReader() >> Mock(BitcoinReader) {
                1 * getTx(txid) >> Mono.just([
                        txid: txid
                ])
            }
        }
        when:
        def act = track.loadExisting(upstream, txid)

        then:
        StepVerifier.create(act)
                .expectNextMatches {
                    it.found == true && it.mined == false && it.blockHash == null
                }
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "loadExiting() returns block if mined"() {
        setup:
        TrackBitcoinTx track = new TrackBitcoinTx(Stub(MultistreamHolder))
        def txid = "69cd44d7c641db82e69824523c7ac0c5c1e5628f025474529cf5ffe64527efc9"
        BitcoinMultistream upstream = Mock(BitcoinMultistream) {
            _ * getReader() >> Mock(BitcoinReader) {
                1 * getTx(txid) >> Mono.just([
                        txid     : txid,
                        blockhash: "0000000000000000000895d1b9d3898700e1deecc3b0e69f439aa77875e6042f",
                        height   : 100
                ])
            }
        }
        when:
        def act = track.loadExisting(upstream, txid)

        then:
        StepVerifier.create(act)
                .expectNextMatches {
                    it.found == true && it.mined == true &&
                            it.blockHash == "0000000000000000000895d1b9d3898700e1deecc3b0e69f439aa77875e6042f" &&
                            it.height == 100
                }
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Goes with confirmations"() {
        setup:
        TrackBitcoinTx track = new TrackBitcoinTx(Stub(MultistreamHolder))
        def txid = "69cd44d7c641db82e69824523c7ac0c5c1e5628f025474529cf5ffe64527efc9"
        // start with the current block
        def next = Flux.fromIterable([10, 12, 13, 14, 15]).map { h ->
            new BlockContainer(h.longValue(), BlockId.from("0000000000000000000895d1b9d3898700e1deecc3b0e69f439aa77875e6042f"), BigInteger.ONE, Instant.now(), false, null, null, [], 0, "unknown")
        }
        Head head = Mock(Head) {
            1 * getFlux() >> next
        }
        BitcoinMultistream upstream = Mock(BitcoinMultistream) {
            1 * getHead() >> head
        }
        def status = new TrackBitcoinTx.TxStatus(
                txid, true, 10, true, "0000000000000000000895d1b9d3898700e1deecc3b0e69f439aa77875e6042f", Instant.now(), BigInteger.ONE, 0
        )
        when:
        def act = track.withConfirmations(upstream, status)

        then:
        StepVerifier.create(act)
                .expectNextMatches { it.confirmations == 1 }
                .expectNextMatches { it.confirmations == 3 }
                .expectNextMatches { it.confirmations == 4 }
                .expectNextMatches { it.confirmations == 5 }
                .expectNextMatches { it.confirmations == 6 }
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Wait until mined"() {
        setup:
        TrackBitcoinTx track = new TrackBitcoinTx(Stub(MultistreamHolder))
        def txid = "69cd44d7c641db82e69824523c7ac0c5c1e5628f025474529cf5ffe64527efc9"
        // start with the current block
        def next = Flux.fromIterable([10, 12, 13]).map { h ->
            new BlockContainer(h.longValue(), BlockId.from("0000000000000000000895d1b9d3898700e1deecc3b0e69f439aa77875e6042f"), BigInteger.ONE, Instant.now(), false, null, null, [], 0, "unknown")
        }
        Head head = Mock(Head) {
            1 * getFlux() >> next
        }
        BitcoinReader api = Mock(BitcoinReader) {
            3 * getTx(txid) >>> [
                    Mono.just([
                            txid: txid
                    ]),
                    Mono.just([
                            txid: txid
                    ]),
                    Mono.just([
                            txid     : txid,
                            blockhash: "0000000000000000000895d1b9d3898700e1deecc3b0e69f439aa77875e6042f",
                            height   : 100
                    ])
            ]
        }
        BitcoinMultistream upstream = Mock(BitcoinMultistream) {
            1 * getHead() >> head
            _ * getReader() >> api
        }
        def status = new TrackBitcoinTx.TxStatus(
                txid, false, null, false, null, null, null, 0
        )
        when:
        def act = track.untilMined(upstream, status)

        then:
        StepVerifier.create(act)
                .expectNextMatches { it.mined && it.height == 100 && it.blockHash == "0000000000000000000895d1b9d3898700e1deecc3b0e69f439aa77875e6042f" }
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Check mempool until found"() {
        setup:
        TrackBitcoinTx track = new TrackBitcoinTx(Stub(MultistreamHolder))
        def txid = "69cd44d7c641db82e69824523c7ac0c5c1e5628f025474529cf5ffe64527efc9"

        Head head = Mock(Head) {
            _ * getFlux() >> Flux.empty()
        }
        CachingMempoolData mempoolAccess = Mock(CachingMempoolData) {
            4 * get() >>> [
                    Mono.just([]),
                    Mono.just(["4523c7ac0c5c1e5628f025474529c69cd44d7c641db82e6982f5ffe64527efc9"]),
                    Mono.just(["4523c7ac0c5c1e5628f025474529c69cd44d7c641db82e6982f5ffe64527efc9", txid]),
                    Mono.just(["4523c7ac0c5c1e5628f025474529c69cd44d7c641db82e6982f5ffe64527efc9", txid]) //second call when started over
            ]
        }
        BitcoinReader api = Mock(BitcoinReader) {
            1 * getTx(txid) >> Mono.just([
                    txid: txid
            ])
            _ * getMempool() >> mempoolAccess
        }
        BitcoinMultistream upstream = Mock(BitcoinMultistream) {
            _ * getHead() >> head
            _ * getReader() >> api
        }

        when:
        def steps = StepVerifier.withVirtualTime {
            track.untilFound(Chain.BITCOIN, upstream, txid).take(1)
        }

        then:
        steps
                .expectSubscription()
                .expectNoEvent(Duration.ofSeconds(3))
                .expectNextMatches { it.found && !it.mined }
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Subscribe to an existing tx"() {
        setup:
        TrackBitcoinTx track = new TrackBitcoinTx(Stub(MultistreamHolder))
        def txid = "69cd44d7c641db82e69824523c7ac0c5c1e5628f025474529cf5ffe64527efc9"
        BitcoinReader api = Mock(BitcoinReader) {
            _ * getTx(txid) >> Mono.just([
                    txid     : txid,
                    blockhash: "0000000000000000000895d1b9d3898700e1deecc3b0e69f439aa77875e6042f",
                    height   : 1
            ])
            _ * getBlock("0000000000000000000895d1b9d3898700e1deecc3b0e69f439aa77875e6042f") >> Mono.just([
                    height   : 1,
                    chainwork: "01",
                    time     : 10000
            ])
        }
        def next = Flux.fromIterable([10, 11, 12]).map { h ->
            new BlockContainer(h.longValue(), BlockId.from("0000000000000000000895d1b9d3898700e1deecc3b0e69f439aa77875e6042f"), BigInteger.ONE, Instant.now(), false, null, null, [], 0, "unknown")
        }
        Head head = Mock(Head) {
            _ * getFlux() >> next
        }
        BitcoinMultistream upstream = Mock(BitcoinMultistream) {
            _ * getReader() >> api
            _ * getHead() >> head
        }

        when:
        def act = track.subscribe(Chain.BITCOIN, upstream, txid)

        then:
        StepVerifier.create(act)
                .expectNextMatches { it.found && it.mined && it.confirmations == 10 && it.blockHash == "0000000000000000000895d1b9d3898700e1deecc3b0e69f439aa77875e6042f" }
                .expectNextMatches { it.found && it.mined && it.confirmations == 11 }
                .expectNextMatches { it.found && it.mined && it.confirmations == 12 }
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }
}
