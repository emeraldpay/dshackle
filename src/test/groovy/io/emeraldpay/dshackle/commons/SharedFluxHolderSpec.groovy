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

import reactor.core.publisher.Flux
import spock.lang.Specification

import java.time.Duration

class SharedFluxHolderSpec extends Specification {

    def "Keeps one flux"() {
        when:
        def called = 0
        def shared = new SharedFluxHolder<Integer>({
            Flux.fromIterable([100+called, 200+called, 300+called, 400+called, 500+called])
                    .delayElements(Duration.ofMillis(100))
                    .doOnSubscribe {called++ }
        })
        List<Integer> values1 = []
        List<Integer> values2 = []
        new Thread({
            values1 = shared.get().collectList().block(Duration.ofSeconds(1))
        }).start()
        new Thread({
            values2 = shared.get().collectList().block(Duration.ofSeconds(1))
        }).start()
        Thread.sleep(1000)

        then:
        values1 == [100, 200, 300, 400, 500]
        values2 == [100, 200, 300, 400, 500]
        called == 1
    }

    def "Create a new flux if existing completes"() {
        when:
        def called = 0
        def shared = new SharedFluxHolder<Integer>({
            Flux.fromIterable([100+called, 200+called, 300+called, 400+called, 500+called])
                    .delayElements(Duration.ofMillis(100))
                    .doOnSubscribe {called++ }
        })
        List<Integer> values1 = []
        List<Integer> values2 = []
        List<Integer> values3 = []
        new Thread({
            values1 = shared.get().collectList().block(Duration.ofSeconds(1))
        }).start()
        new Thread({
            values2 = shared.get().collectList().block(Duration.ofSeconds(1))
        }).start()
        Thread.sleep(1000)

        new Thread({
            values3 = shared.get().collectList().block(Duration.ofSeconds(1))
        }).start()
        Thread.sleep(1000)

        then:
        values1 == [100, 200, 300, 400, 500]
        values2 == [100, 200, 300, 400, 500]
        values3 == [101, 201, 301, 401, 501]
    }

}
