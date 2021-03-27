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
package io.emeraldpay.dshackle.upstream

import org.springframework.context.Lifecycle
import reactor.core.publisher.Flux
import spock.lang.Specification

class MergedHeadSpec extends Specification {

    def "ensures that heads are running on start"() {
        setup:
        def head1 = Stub(TestHead1) {
            _ * getFlux() >> Flux.empty()
        }
        def head2 = Mock(TestHead2) {
            _ * isRunning() >> true
            _ * getFlux() >> Flux.empty()
        }
        def head3 = Mock(TestHead2) {
            _ * isRunning() >> false
            _ * getFlux() >> Flux.empty()
        }

        when:
        def merged = new MergedHead([head1, head2, head3])
        merged.start()

        then:
        1 * head3.start()
    }

    class TestHead1 extends AbstractHead {

    }

    class TestHead2 extends AbstractHead implements Lifecycle {

        @Override
        void start() {

        }

        @Override
        void stop() {

        }

        @Override
        boolean isRunning() {
            return false
        }
    }
}
