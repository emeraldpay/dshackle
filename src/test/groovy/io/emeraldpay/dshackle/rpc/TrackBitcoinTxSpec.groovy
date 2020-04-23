package io.emeraldpay.dshackle.rpc

import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.Upstreams
import io.emeraldpay.dshackle.upstream.bitcoin.BitcoinApi
import io.emeraldpay.grpc.Chain
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Duration
import java.time.Instant

class TrackBitcoinTxSpec extends Specification {

    def "loadMempool() returns not found when not found"() {
        setup:
        TrackBitcoinTx track = new TrackBitcoinTx(Stub(Upstreams))
        BitcoinApi api = Mock(BitcoinApi) {
            1 * getMempool() >> Mono.just([
                    "69cd44d7c641db82e69824523c7ac0c5c1e5628f025474529cf5ffe64527efc9",
                    "d296c6d47335a7f283574b06f1d6303b30ac75631e081ab128346a549ad93350"
            ])
        }
        when:
        def act = track.loadMempool(api, "65ce58db064bd105b14dc76a0bce0df14653cf5263d22d17e78864cf272ee367")

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
        TrackBitcoinTx track = new TrackBitcoinTx(Stub(Upstreams))
        BitcoinApi api = Mock(BitcoinApi) {
            1 * getMempool() >> Mono.just([
                    "69cd44d7c641db82e69824523c7ac0c5c1e5628f025474529cf5ffe64527efc9",
                    "d296c6d47335a7f283574b06f1d6303b30ac75631e081ab128346a549ad93350"
            ])
        }
        when:
        def act = track.loadMempool(api, "69cd44d7c641db82e69824523c7ac0c5c1e5628f025474529cf5ffe64527efc9")

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
        TrackBitcoinTx track = new TrackBitcoinTx(Stub(Upstreams))
        def txid = "69cd44d7c641db82e69824523c7ac0c5c1e5628f025474529cf5ffe64527efc9"
        BitcoinApi api = Mock(BitcoinApi) {
            1 * getTx(txid) >> Mono.just([
                    txid: txid
            ])
        }
        when:
        def act = track.loadExisting(api, txid)

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
        TrackBitcoinTx track = new TrackBitcoinTx(Stub(Upstreams))
        def txid = "69cd44d7c641db82e69824523c7ac0c5c1e5628f025474529cf5ffe64527efc9"
        BitcoinApi api = Mock(BitcoinApi) {
            1 * getTx(txid) >> Mono.just([
                    txid     : txid,
                    blockhash: "0000000000000000000895d1b9d3898700e1deecc3b0e69f439aa77875e6042f",
                    height   : 100
            ])
        }
        when:
        def act = track.loadExisting(api, txid)

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
        TrackBitcoinTx track = new TrackBitcoinTx(Stub(Upstreams))
        def txid = "69cd44d7c641db82e69824523c7ac0c5c1e5628f025474529cf5ffe64527efc9"
        // start with the current block
        def next = Flux.fromIterable([10, 12, 13, 14, 15]).map { h ->
            new BlockContainer(h.longValue(), BlockId.from("0000000000000000000895d1b9d3898700e1deecc3b0e69f439aa77875e6042f"), BigInteger.ONE, Instant.now(), false, null, [])
        }
        Head head = Mock(Head) {
            1 * getFlux() >> next
        }
        Upstream upstream = Mock(Upstream) {
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
        TrackBitcoinTx track = new TrackBitcoinTx(Stub(Upstreams))
        def txid = "69cd44d7c641db82e69824523c7ac0c5c1e5628f025474529cf5ffe64527efc9"
        // start with the current block
        def next = Flux.fromIterable([10, 12, 13]).map { h ->
            new BlockContainer(h.longValue(), BlockId.from("0000000000000000000895d1b9d3898700e1deecc3b0e69f439aa77875e6042f"), BigInteger.ONE, Instant.now(), false, null, [])
        }
        Head head = Mock(Head) {
            1 * getFlux() >> next
        }
        BitcoinApi api = Mock(BitcoinApi) {
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
        Upstream upstream = Mock(Upstream) {
            1 * getHead() >> head
            _ * getApi(_) >> Mono.just(api)
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
        TrackBitcoinTx track = new TrackBitcoinTx(Stub(Upstreams))
        def txid = "69cd44d7c641db82e69824523c7ac0c5c1e5628f025474529cf5ffe64527efc9"
        BitcoinApi api = Mock(BitcoinApi) {
            4 * getMempool() >>> [
                    Mono.just([]),
                    Mono.just(["4523c7ac0c5c1e5628f025474529c69cd44d7c641db82e6982f5ffe64527efc9"]),
                    Mono.just(["4523c7ac0c5c1e5628f025474529c69cd44d7c641db82e6982f5ffe64527efc9", txid]),
                    Mono.just(["4523c7ac0c5c1e5628f025474529c69cd44d7c641db82e6982f5ffe64527efc9", txid]) //second call when started over
            ]
            1 * getTx(txid) >> Mono.just([
                    txid: txid
            ])
        }
        Head head = Mock(Head) {
            _ * getFlux() >> Flux.empty()
        }
        Upstream upstream = Mock(Upstream) {
            _ * getApi(_) >> Mono.just(api)
            _ * getHead() >> head
        }

        when:
        def steps = StepVerifier.withVirtualTime {
            track.untilFound(Chain.BITCOIN, api, upstream, txid).take(1)
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
        TrackBitcoinTx track = new TrackBitcoinTx(Stub(Upstreams))
        def txid = "69cd44d7c641db82e69824523c7ac0c5c1e5628f025474529cf5ffe64527efc9"
        BitcoinApi api = Mock(BitcoinApi) {
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
            new BlockContainer(h.longValue(), BlockId.from("0000000000000000000895d1b9d3898700e1deecc3b0e69f439aa77875e6042f"), BigInteger.ONE, Instant.now(), false, null, [])
        }
        Head head = Mock(Head) {
            _ * getFlux() >> next
        }
        Upstream upstream = Mock(Upstream) {
            _ * getApi(_) >> Mono.just(api)
            _ * getHead() >> head
        }

        when:
        def act = track.subscribe(Chain.BITCOIN, api, upstream, txid)

        then:
        StepVerifier.create(act)
                .expectNextMatches { it.found && it.mined && it.confirmations == 10 && it.blockHash == "0000000000000000000895d1b9d3898700e1deecc3b0e69f439aa77875e6042f" }
                .expectNextMatches { it.found && it.mined && it.confirmations == 11 }
                .expectNextMatches { it.found && it.mined && it.confirmations == 12 }
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }
}
