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
package io.emeraldpay.dshackle.quorum

import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcException
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.dshackle.upstream.signature.ResponseSigner
import spock.lang.Specification

class AlwaysQuorumSpec extends Specification {

    def "Failed if error received"() {
        setup:
        def quorum = new AlwaysQuorum()
        def up = Stub(Upstream)
        when:
        quorum.record(new JsonRpcException(1, "test"), null, up)
        then:
        quorum.isFailed()
        !quorum.isResolved()
        quorum.getError() != null
        with(quorum.getError()) {
            message == "test"
        }
    }

    def "Resolved if result received"() {
        setup:
        def quorum = new AlwaysQuorum()
        def up = Stub(Upstream)
        when:
        quorum.record(new JsonRpcResponse("123".bytes, null), new ResponseSigner.Signature("sig1".bytes, "test", 100), up)
        then:
        quorum.isResolved()
        quorum.getResponse().getResult() == "123".bytes
        quorum.signature == new ResponseSigner.Signature("sig1".bytes, "test", 100)
        !quorum.isFailed()
    }
}
