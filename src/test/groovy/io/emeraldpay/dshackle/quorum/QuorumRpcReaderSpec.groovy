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

import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.upstream.FilteredApis
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcException
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.grpc.Chain
import io.emeraldpay.etherjar.rpc.RpcException
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Duration

class QuorumRpcReaderSpec extends Specification {

    def "always-quorum - get the result if ok"() {
        setup:
        def up = Mock(Upstream) {
            _ * isAvailable() >> true
            _ * getRole() >> UpstreamsConfig.UpstreamRole.PRIMARY
            1 * getApi() >> Mock(Reader) {
                1 * read(new JsonRpcRequest("eth_test", [])) >> Mono.just(JsonRpcResponse.ok("1"))
            }
        }
        def apis = new FilteredApis(
                Chain.ETHEREUM,
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

    def "always-quorum - return upstream error"() {
        setup:
        def api = Mock(Reader) {
            1 * read(new JsonRpcRequest("eth_test", [])) >>> [
                    Mono.just(JsonRpcResponse.error(1, "test"))
            ]
        }
        def up = Mock(Upstream) {
            _ * isAvailable() >> true
            _ * getRole() >> UpstreamsConfig.UpstreamRole.PRIMARY
            _ * getApi() >> api
        }
        def apis = new FilteredApis(
                Chain.ETHEREUM,
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
                .expectErrorMatches {
                    it instanceof JsonRpcException && ((JsonRpcException) it).error.message == "test"
                }
                .verify(Duration.ofSeconds(1))
    }

    def "non-empty-quorum - get the second result if first is null"() {
        setup:
        def up = Mock(Upstream) {
            _ * isAvailable() >> true
            _ * getRole() >> UpstreamsConfig.UpstreamRole.PRIMARY
            _ * getApi() >> Mock(Reader) {
                2 * read(new JsonRpcRequest("eth_test", [])) >>> [
                        Mono.just(JsonRpcResponse.ok("null")),
                        Mono.just(JsonRpcResponse.ok("1"))
                ]
            }
        }
        def apis = new FilteredApis(
                Chain.ETHEREUM,
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
            _ * getRole() >> UpstreamsConfig.UpstreamRole.PRIMARY
            _ * getApi() >> Mock(Reader) {
                2 * read(new JsonRpcRequest("eth_test", [])) >>> [
                        Mono.just(JsonRpcResponse.error(1, "test")),
                        Mono.just(JsonRpcResponse.ok("1"))
                ]
            }
        }
        def apis = new FilteredApis(
                Chain.ETHEREUM,
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
            _ * getRole() >> UpstreamsConfig.UpstreamRole.PRIMARY
            _ * getApi() >> Mock(Reader) {
                3 * read(new JsonRpcRequest("eth_test", [])) >>> [
                        Mono.just(JsonRpcResponse.ok("null")),
                        Mono.just(JsonRpcResponse.error(1, "test")),
                        Mono.just(JsonRpcResponse.ok("1"))
                ]
            }
        }
        def apis = new FilteredApis(
                Chain.ETHEREUM,
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

    def "non-empty-quorum - error if all failed"() {
        setup:
        def api = Mock(Reader) {
            3 * read(new JsonRpcRequest("eth_test", [])) >>> [
                    Mono.just(JsonRpcResponse.ok("null")),
                    Mono.just(JsonRpcResponse.error(1, "test")),
                    Mono.just(JsonRpcResponse.ok("null"))
            ]
        }
        def up = Mock(Upstream) {
            _ * isAvailable() >> true
            _ * getRole() >> UpstreamsConfig.UpstreamRole.PRIMARY
            _ * getApi() >> api
        }
        def apis = new FilteredApis(
                Chain.ETHEREUM,
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
                .expectError()
                .verify(Duration.ofSeconds(2))
    }

    def "Return error is upstream returned it"() {
        setup:
        def up = Mock(Upstream) {
            _ * getLag() >> 0
            _ * isAvailable() >> true
            _ * getRole() >> UpstreamsConfig.UpstreamRole.PRIMARY
            _ * getApi() >> Mock(Reader) {
                _ * read(new JsonRpcRequest("eth_test", [])) >>> [
                        Mono.just(JsonRpcResponse.error(-3010, "test")),
                ]
            }
        }
        def apis = new FilteredApis(
                Chain.ETHEREUM,
                [up], Selector.empty
        )
        def reader = new QuorumRpcReader(apis, new NotLaggingQuorum(1))

        when:
        def act = reader.read(new JsonRpcRequest("eth_test", []))

        then:
        StepVerifier.create(act)
                .expectError()
        //TODO verify
        //.expectErrorMatches { t -> t instanceof RpcException && t.code == -3010}
                .verify(Duration.ofSeconds(1))
    }

}
