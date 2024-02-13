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
package io.emeraldpay.dshackle.upstream.ethereum.subscribe

import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.ethereum.domain.TransactionId
import reactor.core.publisher.Flux
import spock.lang.Specification

import java.time.Duration

class AggregatedPendingTxesSpec extends Specification {

    def "Produces values from two sources"() {
        setup:
        def source1 = Mock(PendingTxesSource)
        def source2 = Mock(PendingTxesSource)

        when:
        def aggregate = new AggregatedPendingTxes([source1, source2])
        def values = aggregate.connect(Selector.empty)
                .collectList().block(Duration.ofSeconds(1))

        then:
        1 * source1.connect(Selector.empty) >> Flux.fromIterable(
                [
                        "0xa61bab14fc9720ea8725622688c2f964666d7c2afdae38af7dad53f12f242d5c",
                        "0x911548eb0f3bf353a54e03a3506c7c3e747470d6c201f03babbc07ff6e14cd6e"
                ].collect { TransactionId.from(it) }
        )
        1 * source2.connect(Selector.empty) >> Flux.fromIterable(
                [
                        "0x9a9d4618b12d36d17a63d48c5b5efc05b461feead124ddd86803d8bca4015248",
                        "0xa38173981f8eab96ee70cefe42735af0f574b7ef354565f2fea32a28e5ed9bd2"
                ].collect { TransactionId.from(it) }
        )

        values.collect { it.toHex() }.toSorted() == [
                "0xa61bab14fc9720ea8725622688c2f964666d7c2afdae38af7dad53f12f242d5c",
                "0x911548eb0f3bf353a54e03a3506c7c3e747470d6c201f03babbc07ff6e14cd6e",
                "0x9a9d4618b12d36d17a63d48c5b5efc05b461feead124ddd86803d8bca4015248",
                "0xa38173981f8eab96ee70cefe42735af0f574b7ef354565f2fea32a28e5ed9bd2"
        ].toSorted()
    }

    def "Skip duplicates"() {
        setup:
        def source1 = Mock(PendingTxesSource)
        def source2 = Mock(PendingTxesSource)

        when:
        def aggregate = new AggregatedPendingTxes([source1, source2])
        def values = aggregate.connect(Selector.empty)
                .collectList().block(Duration.ofSeconds(1))

        then:
        1 * source1.connect(Selector.empty) >> Flux.fromIterable(
                [
                        "0xa61bab14fc9720ea8725622688c2f964666d7c2afdae38af7dad53f12f242d5c",
                        "0x9a9d4618b12d36d17a63d48c5b5efc05b461feead124ddd86803d8bca4015248",
                        "0x911548eb0f3bf353a54e03a3506c7c3e747470d6c201f03babbc07ff6e14cd6e"
                ].collect { TransactionId.from(it) }
        )
        1 * source2.connect(Selector.empty) >> Flux.fromIterable(
                [
                        "0x9a9d4618b12d36d17a63d48c5b5efc05b461feead124ddd86803d8bca4015248",
                        "0xa61bab14fc9720ea8725622688c2f964666d7c2afdae38af7dad53f12f242d5c",
                        "0xa38173981f8eab96ee70cefe42735af0f574b7ef354565f2fea32a28e5ed9bd2"
                ].collect { TransactionId.from(it) }
        )

        values.collect { it.toHex() }.toSorted() == [
                "0xa61bab14fc9720ea8725622688c2f964666d7c2afdae38af7dad53f12f242d5c",
                "0x911548eb0f3bf353a54e03a3506c7c3e747470d6c201f03babbc07ff6e14cd6e",
                "0x9a9d4618b12d36d17a63d48c5b5efc05b461feead124ddd86803d8bca4015248",
                "0xa38173981f8eab96ee70cefe42735af0f574b7ef354565f2fea32a28e5ed9bd2"
        ].toSorted()
    }
}
