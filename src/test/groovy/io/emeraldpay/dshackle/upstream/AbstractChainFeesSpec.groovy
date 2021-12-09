package io.emeraldpay.dshackle.upstream

import kotlin.jvm.functions.Function1
import org.jetbrains.annotations.NotNull
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import spock.lang.Specification

import java.time.Duration
import java.util.function.Function

class AbstractChainFeesSpec extends Specification {

    Function<List<String>, List<String>> extractTx = { it }

    def "Iterates N last blocks"() {
        setup:
        def ups = Mock(Multistream) {
            1 * getHead() >> Mock(Head) {
                1 * it.getCurrentHeight() >> 234
            }
        }
        def fees = new TestChainFees(5, ups, Stub(Function1))
        when:
        def act = fees.usingBlocks(3).collectList().block(Duration.ofSeconds(1)).toSorted()
        then:
        act == [232L, 233L, 234L]
    }

    def "Limits iteration to configured"() {
        setup:
        def ups = Mock(Multistream) {
            1 * getHead() >> Mock(Head) {
                1 * it.getCurrentHeight() >> 234
            }
        }
        def fees = new TestChainFees(3, ups, Stub(Function1))
        when:
        def act = fees.usingBlocks(5).collectList().block(Duration.ofSeconds(1)).toSorted()
        then:
        act == [232L, 233L, 234L]
    }

    def "TxAtPos 0 for empty list"() {
        setup:
        def txAt = new AbstractChainFees.TxAtPos<List<String>, String>(extractTx, 0)

        when:
        def act = txAt.get([])

        then:
        act == null
    }

    def "TxAtPos 0 for single item list"() {
        setup:
        def txAt = new AbstractChainFees.TxAtPos<List<String>, String>(extractTx, 0)

        when:
        def act = txAt.get(["t_1"])

        then:
        act == "t_1"
    }

    def "TxAtPos 0 for many item list"() {
        setup:
        def txAt = new AbstractChainFees.TxAtPos<List<String>, String>(extractTx, 0)

        when:
        def act = txAt.get(["t_1", "t_2", "t_3"])

        then:
        act == "t_3"
    }

    def "TxAtPos 1 for many item list"() {
        setup:
        def txAt = new AbstractChainFees.TxAtPos<List<String>, String>(extractTx, 1)

        when:
        def act = txAt.get(["t_1", "t_2", "t_3"])

        then:
        act == "t_2"
    }

    def "TxAtPos 2 for many item list"() {
        setup:
        def txAt = new AbstractChainFees.TxAtPos<List<String>, String>(extractTx, 2)

        when:
        def act = txAt.get(["t_1", "t_2", "t_3"])

        then:
        act == "t_1"
    }

    def "TxAtPos 5 for 3 item list"() {
        setup:
        def txAt = new AbstractChainFees.TxAtPos<List<String>, String>(extractTx, 5)

        when:
        def act = txAt.get(["t_1", "t_2", "t_3"])

        then:
        act == "t_1"
    }

    def "TxAtBottom for empty list"() {
        setup:
        def txAt = new AbstractChainFees.TxAtBottom<List<String>, String>(extractTx)

        when:
        def act = txAt.get([])

        then:
        act == null
    }

    def "TxAtBottom for single item list"() {
        setup:
        def txAt = new AbstractChainFees.TxAtBottom<List<String>, String>(extractTx)

        when:
        def act = txAt.get(["t_1"])

        then:
        act == "t_1"
    }

    def "TxAtBottom for multi item list"() {
        setup:
        def txAt = new AbstractChainFees.TxAtBottom<List<String>, String>(extractTx)

        when:
        def act = txAt.get(["t_1", "t_2", "t_3"])

        then:
        act == "t_3"
    }

    def "TxAtTop for empty list"() {
        setup:
        def txAt = new AbstractChainFees.TxAtTop<List<String>, String>(extractTx)

        when:
        def act = txAt.get([])

        then:
        act == null
    }

    def "TxAtTop for single item list"() {
        setup:
        def txAt = new AbstractChainFees.TxAtTop<List<String>, String>(extractTx)

        when:
        def act = txAt.get(["t_1"])

        then:
        act == "t_1"
    }

    def "TxAtTop for multi item list"() {
        setup:
        def txAt = new AbstractChainFees.TxAtTop<List<String>, String>(extractTx)

        when:
        def act = txAt.get(["t_1", "t_2", "t_3"])

        then:
        act == "t_1"
    }

    def "TxAtMiddle for empty list"() {
        setup:
        def txAt = new AbstractChainFees.TxAtMiddle<List<String>, String>(extractTx)

        when:
        def act = txAt.get([])

        then:
        act == null
    }

    def "TxAtMiddle for single item list"() {
        setup:
        def txAt = new AbstractChainFees.TxAtMiddle<List<String>, String>(extractTx)

        when:
        def act = txAt.get(["t_1"])

        then:
        act == "t_1"
    }

    def "TxAtMiddle for 3 item list"() {
        setup:
        def txAt = new AbstractChainFees.TxAtMiddle<List<String>, String>(extractTx)

        when:
        def act = txAt.get(["t_1", "t_2", "t_3"])

        then:
        act == "t_2"
    }

    def "TxAtMiddle for 4 item list"() {
        setup:
        def txAt = new AbstractChainFees.TxAtMiddle<List<String>, String>(extractTx)

        when:
        def act = txAt.get(["t_1", "t_2", "t_3", "t_4"])

        then:
        act == "t_2" || act == "t_3"
    }

    def "TxAtMiddle for 5 item list"() {
        setup:
        def txAt = new AbstractChainFees.TxAtMiddle<List<String>, String>(extractTx)

        when:
        def act = txAt.get(["t_1", "t_2", "t_3", "t_4", "t_5"])

        then:
        act == "t_3"
    }

    class TestChainFees extends AbstractChainFees {

        TestChainFees(int heightLimit, @NotNull Multistream upstreams, @NotNull Function1 extractTx) {
            super(heightLimit, upstreams, extractTx)
        }

        @Override
        Mono readFeesAt(long height, @NotNull TxAt selector) {
            return null
        }

        @Override
        Function<Flux, Mono> feeAggregation(@NotNull Mode mode) {
            return null
        }

        @Override
        Function getResponseBuilder() {
            return null
        }
    }

}
