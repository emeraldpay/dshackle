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

import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.quorum.NotLaggingQuorum
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcException
import io.emeraldpay.etherjar.rpc.RpcException
import spock.lang.Specification

class NotLaggingQuorumSpec extends Specification {

    def "Resolves if no lag"() {
        setup:
        def up = Mock(Upstream)
        def value = "foo".getBytes()
        def quorum = new NotLaggingQuorum(1)

        when:
        quorum.record(value, "sig1", up)
        then:
        1 * up.getLag() >> 0
        quorum.isResolved()
        !quorum.isFailed()
        quorum.result == value
        quorum.signature == "sig1"
    }

    def "Resolves if ok lag"() {
        setup:
        def up = Mock(Upstream)
        def value = "foo".getBytes()
        def quorum = new NotLaggingQuorum(1)

        when:
        quorum.record(value,"sig1", up)
        then:
        1 * up.getLag() >> 1
        quorum.isResolved()
        !quorum.isFailed()
        quorum.result == value
        quorum.signature == "sig1"
    }

    def "Ignores if lags"() {
        setup:
        def up = Mock(Upstream)
        def value = "foo".getBytes()
        def quorum = new NotLaggingQuorum(1)

        when:
        quorum.record(value, "sig1", up)
        then:
        1 * up.getLag() >> 2
        !quorum.isResolved()
        !quorum.isFailed()
        quorum.signature == ""
    }

    def "Fails if no lag and error response received"() {
        setup:
        def up = Mock(Upstream)
        def value = "foo".getBytes()
        def quorum = new NotLaggingQuorum(1)

        when:
        quorum.record(new JsonRpcException(-100, "test error"), "sig1", up)
        then:
        1 * up.getLag() >> 1
        !quorum.isResolved()
        quorum.isFailed()
        quorum.signature == ""
    }
}
