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

import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.upstream.FilteredApis
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Duration

class QuorumRpcReaderSpec extends Specification {

    def "always-quorum - get the result if ok"() {
        setup:
        def up = Mock(Upstream) {
            _ * isAvailable() >> true
            1 * getApi() >> Mock(Reader) {
                1 * read(new JsonRpcRequest("eth_test", [])) >> Mono.just(JsonRpcResponse.ok("1"))
            }
        }
        def apis = new FilteredApis(
                [up], Selector.empty
        )
        def reader = new QuorumRpcReader(apis, new AlwaysQuorum())

        when:
        def act = reader.read(new JsonRpcRequest("eth_test", []))
                .map {
                    new String(it.value)
                }

        then:
        StepVerifier.create(act)
                .expectNext("1")
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "always-quorum - retry upstream error"() {
        setup:
        def up = Mock(Upstream) {
            _ * isAvailable() >> true
            _ * getApi() >> Mock(Reader) {
                2 * read(new JsonRpcRequest("eth_test", [])) >>> [
                        Mono.just(JsonRpcResponse.error(1, "test")),
                        Mono.just(JsonRpcResponse.ok("1"))
                ]
            }
        }
        def apis = new FilteredApis(
                [up], Selector.empty
        )
        def reader = new QuorumRpcReader(apis, new AlwaysQuorum())

        when:
        def act = reader.read(new JsonRpcRequest("eth_test", []))
                .map {
                    new String(it.value)
                }

        then:
        StepVerifier.create(act)
                .expectNext("1")
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "non-empty-quorum - get the second result if first is null"() {
        setup:
        def up = Mock(Upstream) {
            _ * isAvailable() >> true
            _ * getApi() >> Mock(Reader) {
                2 * read(new JsonRpcRequest("eth_test", [])) >>> [
                        Mono.just(JsonRpcResponse.ok("null")),
                        Mono.just(JsonRpcResponse.ok("1"))
                ]
            }
        }
        def apis = new FilteredApis(
                [up], Selector.empty
        )
        def reader = new QuorumRpcReader(apis, new NonEmptyQuorum(3))

        when:
        def act = reader.read(new JsonRpcRequest("eth_test", []))
                .map {
                    new String(it.value)
                }

        then:
        StepVerifier.create(act)
                .expectNext("1")
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }


    def "non-empty-quorum - get the second result if first is error"() {
        setup:
        def up = Mock(Upstream) {
            _ * isAvailable() >> true
            _ * getApi() >> Mock(Reader) {
                2 * read(new JsonRpcRequest("eth_test", [])) >>> [
                        Mono.just(JsonRpcResponse.error(1, "test")),
                        Mono.just(JsonRpcResponse.ok("1"))
                ]
            }
        }
        def apis = new FilteredApis(
                [up], Selector.empty
        )
        def reader = new QuorumRpcReader(apis, new NonEmptyQuorum(3))

        when:
        def act = reader.read(new JsonRpcRequest("eth_test", []))
                .map {
                    new String(it.value)
                }

        then:
        StepVerifier.create(act)
                .expectNext("1")
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "non-empty-quorum - get the third result if first two are not ok"() {
        setup:
        def up = Mock(Upstream) {
            _ * isAvailable() >> true
            _ * getApi() >> Mock(Reader) {
                3 * read(new JsonRpcRequest("eth_test", [])) >>> [
                        Mono.just(JsonRpcResponse.ok("null")),
                        Mono.just(JsonRpcResponse.error(1, "test")),
                        Mono.just(JsonRpcResponse.ok("1"))
                ]
            }
        }
        def apis = new FilteredApis(
                [up], Selector.empty
        )
        def reader = new QuorumRpcReader(apis, new NonEmptyQuorum(3))

        when:
        def act = reader.read(new JsonRpcRequest("eth_test", []))
                .map {
                    new String(it.value)
                }

        then:
        StepVerifier.create(act)
                .expectNext("1")
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "non-empty-quorum - no result if all failed"() {
        setup:
        def up = Mock(Upstream) {
            _ * isAvailable() >> true
            _ * getApi() >> Mock(Reader) {
                3 * read(new JsonRpcRequest("eth_test", [])) >>> [
                        Mono.just(JsonRpcResponse.ok("null")),
                        Mono.just(JsonRpcResponse.error(1, "test")),
                        Mono.just(JsonRpcResponse.ok("null"))
                ]
            }
        }
        def apis = new FilteredApis(
                [up], Selector.empty
        )
        def reader = new QuorumRpcReader(apis, new NonEmptyQuorum(3))

        when:
        def act = reader.read(new JsonRpcRequest("eth_test", []))
                .map {
                    new String(it.value)
                }

        then:
        StepVerifier.create(act)
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

}
