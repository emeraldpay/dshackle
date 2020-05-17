/**
 * Copyright (c) 2019 ETCDEV GmbH
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
package io.emeraldpay.dshackle.quorum

import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.quorum.NonceQuorum
import io.infinitape.etherjar.rpc.RpcException
import spock.lang.Specification

class NonceQuorumSpec extends Specification {

    def objectMapper = TestingCommons.objectMapper()

    def "Gets max value"() {
        setup:
        def q = Spy(new NonceQuorum(objectMapper, 3))
        def upstream1 = Stub(Upstream)
        def upstream2 = Stub(Upstream)
        def upstream3 = Stub(Upstream)

        when:
        q.init(Stub(Head))
        then:
        !q.isResolved()

        when:
        q.record('"0x10"'.bytes, upstream1)
        then:
        !q.isResolved()
        1 * q.recordValue(_, "0x10", _)

        when:
        q.record('"0x11"'.bytes, upstream2)
        then:
        !q.isResolved()
        1 * q.recordValue(_, "0x11", _)

        when:
        q.record('"0x10"'.bytes, upstream3)
        then:
        1 * q.recordValue(_, "0x10", _)
        q.isResolved()
        objectMapper.readValue(q.result, Object) == "0x11"
    }

    def "Ignores errors"() {
        setup:
        def q = Spy(new NonceQuorum(objectMapper, 3))
        def upstream1 = Stub(Upstream)
        def upstream2 = Stub(Upstream)
        def upstream3 = Stub(Upstream)

        when:
        q.init(Stub(Head))
        then:
        !q.isResolved()

        when:
        q.record(new RpcException(1, "Internal"), upstream1)
        then:
        !q.isResolved()
        1 * q.recordError(_, _, _)

        when:
        q.record('"0x11"'.bytes, upstream2)
        then:
        !q.isResolved()
        1 * q.recordValue(_, "0x11", _)

        when:
        q.record('"0x10"'.bytes, upstream3)
        then:
        1 * q.recordValue(_, "0x10", _)
        !q.isResolved()

        when:
        q.record('"0x11"'.bytes, upstream1)
        then:
        1 * q.recordValue(_, "0x11", _)
        q.isResolved()
        objectMapper.readValue(q.result, Object) == "0x11"
    }

    def "Fail if too many errors"() {
        setup:
        def q = Spy(new NonceQuorum(objectMapper, 3))
        def upstream1 = Stub(Upstream)
        def upstream2 = Stub(Upstream)
        def upstream3 = Stub(Upstream)

        when:
        q.init(Stub(Head))
        then:
        !q.isResolved()
        !q.isFailed()

        when:
        q.record(new RpcException(1, "Internal"), upstream1)
        then:
        !q.isResolved()
        !q.isFailed()

        when:
        q.record(new RpcException(1, "Internal"), upstream2)
        then:
        !q.isResolved()
        !q.isFailed()

        when:
        q.record(new RpcException(1, "Internal"), upstream3)
        then:
        q.isFailed()
        !q.isResolved()
    }
}
