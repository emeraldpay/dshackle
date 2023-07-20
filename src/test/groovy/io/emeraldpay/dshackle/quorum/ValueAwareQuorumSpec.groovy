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
package io.emeraldpay.dshackle.quorum

import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.signature.ResponseSigner
import org.jetbrains.annotations.NotNull
import org.jetbrains.annotations.Nullable
import spock.lang.Specification

class ValueAwareQuorumSpec extends Specification {

    def "Extract null"() {
        setup:
        def quorum = new ValueAwareQuorumImpl()
        when:
        def act = quorum.extractValue("null".bytes, Object)
        then:
        act == null
    }

    def "Extract string"() {
        setup:
        def quorum = new ValueAwareQuorumImpl()
        when:
        def act = quorum.extractValue("\"foo\"".bytes, Object)
        then:
        act == "foo"
    }

    def "Extract number"() {
        setup:
        def quorum = new ValueAwareQuorumImpl()
        when:
        def act = quorum.extractValue("100".bytes, Object)
        then:
        act == 100
    }

    def "Extract map"() {
        setup:
        def quorum = new ValueAwareQuorumImpl()
        when:
        def act = quorum.extractValue("{\"foo\": 1}".bytes, Object)
        then:
        act == [foo: 1]
    }

    class ValueAwareQuorumImpl extends ValueAwareQuorum {
        ValueAwareQuorumImpl() {
            super(Object)
        }

        @Override
        void recordValue(@NotNull byte[] response, @Nullable Object responseValue, @Nullable ResponseSigner.Signature signature, @NotNull Upstream upstream) {

        }



        @Override
        void recordError(@Nullable byte[] response, @Nullable String errorMessage, @Nullable ResponseSigner.Signature signature, @NotNull Upstream upstream) {

        }

        @Override
        void init(@NotNull Head head) {

        }

        @Override
        boolean isResolved() {
            return false
        }

        @Override
        ResponseSigner.Signature getSignature() {
            return null
        }

        @Override
        byte[] getResult() {
            return new byte[0]
        }

        @Override
        boolean isFailed() {
            return false
        }
    }
}
