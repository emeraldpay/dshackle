/**
 * Copyright (c) 2020 ETCDEV GmbH
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
package io.emeraldpay.dshackle.proxy

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.TlsSetup
import io.emeraldpay.dshackle.config.ProxyConfig
import io.emeraldpay.dshackle.rpc.NativeCall
import io.emeraldpay.dshackle.test.TestingCommons
import reactor.core.publisher.Flux
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Duration
import java.util.function.Function

class ProxyServerSpec extends Specification {

    def "Uses NativeCall"() {
        setup:
        NativeCall nativeCall = Mock(NativeCall)
        def predefined = { a -> Flux.just("hello") } as Function

        WriteRpcJson writeRpcJson = Mock {
            1 * toJsons(_) >> predefined
        }

        ProxyServer server = new ProxyServer(
                new ProxyConfig(),
                new ReadRpcJson(),
                writeRpcJson,
                nativeCall,
                new TlsSetup(TestingCommons.fileResolver())
        )

        def call = new ProxyCall(ProxyCall.RpcType.SINGLE)
        call.ids[1] = 1
        call.items.add(
                BlockchainOuterClass.NativeCallItem.newBuilder()
                        .setMethod("eth_hello")
                        .build()
        )
        when:
        def act = server.execute(Common.ChainRef.CHAIN_ETHEREUM, call)

        then:
        1 * nativeCall.nativeCall(_) >> Flux.just(BlockchainOuterClass.NativeCallReplyItem.newBuilder().build())
        StepVerifier.create(act)
                .expectNext("hello")
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }
}
