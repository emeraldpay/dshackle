package io.emeraldpay.dshackle.reader

import io.emeraldpay.dshackle.quorum.BroadcastQuorum
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcException
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import org.springframework.cloud.sleuth.Tracer
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Duration

class BroadcastReaderSpec extends Specification {

    def "Return responses from all upstreams"() {
        setup:
        def result = "123".getBytes()
        def up = Mock(Upstream) {
            1 * isAvailable() >> true
            _ * getId() >> "id"
            1 * getIngressReader() >> Mock(Reader) {
                1 * read(new JsonRpcRequest("eth_sendRawTransaction", new ListParams(["0x1"]))) >>
                        Mono.just(new JsonRpcResponse(result, null))
            }
        }
        def up1 = Mock(Upstream) {
            1 * isAvailable() >> true
            _ * getId() >> "id"
            1 * getIngressReader() >> Mock(Reader) {
                1 * read(new JsonRpcRequest("eth_sendRawTransaction", new ListParams(["0x1"]))) >>
                        Mono.just(new JsonRpcResponse(result, null))
            }
        }
        def up2 = Mock(Upstream) {
            1 * isAvailable() >> true
            _ * getId() >> "id"
            1 * getIngressReader() >> Mock(Reader) {
                1 * read(new JsonRpcRequest("eth_sendRawTransaction", new ListParams(["0x1"]))) >>
                        Mono.just(new JsonRpcResponse(result, null))
            }
        }
        def reader = new BroadcastReader([up, up1, up2], new Selector.EmptyMatcher(), null, new BroadcastQuorum(), Stub(Tracer))
        when:
        def act = reader.read(new JsonRpcRequest("eth_sendRawTransaction", new ListParams(["0x1"])))
        then:
        StepVerifier.create(act)
            .expectNextMatches {
                it.value == result
            }
            .expectComplete()
            .verify(Duration.ofSeconds(3))
    }

    def "Return response if at least one upstream responds"() {
        setup:
        def result = "123".getBytes()
        def up = Mock(Upstream) {
            1 * isAvailable() >> true
            _ * getId() >> "id"
            1 * getIngressReader() >> Mock(Reader) {
                1 * read(new JsonRpcRequest("eth_sendRawTransaction", new ListParams(["0x1"]))) >>
                        Mono.just(new JsonRpcResponse(result, null))
            }
        }
        def up1 = Mock(Upstream) {
            1 * isAvailable() >> true
            _ * getId() >> "id"
            1 * getIngressReader() >> Mock(Reader) {
                1 * read(new JsonRpcRequest("eth_sendRawTransaction", new ListParams(["0x1"]))) >>
                        Mono.error(new JsonRpcException(1, "too low"))
            }
        }
        def up2 = Mock(Upstream) {
            1 * isAvailable() >> true
            _ * getId() >> "id"
            1 * getIngressReader() >> Mock(Reader) {
                1 * read(new JsonRpcRequest("eth_sendRawTransaction", new ListParams(["0x1"]))) >>
                        Mono.error(new JsonRpcException(1, "too low"))            }
        }
        def reader = new BroadcastReader([up, up1, up2], new Selector.EmptyMatcher(), null, new BroadcastQuorum(), Stub(Tracer))
        when:
        def act = reader.read(new JsonRpcRequest("eth_sendRawTransaction", new ListParams(["0x1"])))
        then:
        StepVerifier.create(act)
                .expectNextMatches {
                    it.value == result
                }
                .expectComplete()
                .verify(Duration.ofSeconds(3))
    }

    def "Return response from matched upstream"() {
        setup:
        def result = "123".getBytes()
        def up = Mock(Upstream) {
            1 * isAvailable() >> true
            _ * getId() >> "id"
            1 * getIngressReader() >> Mock(Reader) {
                1 * read(new JsonRpcRequest("eth_sendRawTransaction", new ListParams(["0x1"]))) >>
                        Mono.just(new JsonRpcResponse(result, null))
            }
        }
        def up1 = Mock(Upstream) {
            1 * isAvailable() >> false
            0 * getId() >> "id"
            0 * getIngressReader() >> Mock(Reader)
        }
        def up2 = Mock(Upstream) {
            1 * isAvailable() >> false
            0 * getId() >> "id"
            0 * getIngressReader() >> Mock(Reader)
        }
        def reader = new BroadcastReader([up, up1, up2], new Selector.EmptyMatcher(), null, new BroadcastQuorum(), Stub(Tracer))
        when:
        def act = reader.read(new JsonRpcRequest("eth_sendRawTransaction", new ListParams(["0x1"])))
        then:
        StepVerifier.create(act)
                .expectNextMatches {
                    it.value == result
                }
                .expectComplete()
                .verify(Duration.ofSeconds(3))
    }

    def "Return error if all upstreams return error"() {
        setup:
        def up = Mock(Upstream) {
            1 * isAvailable() >> true
            _ * getId() >> "id"
            1 * getIngressReader() >> Mock(Reader) {
                1 * read(new JsonRpcRequest("eth_sendRawTransaction", new ListParams(["0x1"]))) >>
                        Mono.error(new JsonRpcException(1, "too low"))
            }
        }
        def up1 = Mock(Upstream) {
            1 * isAvailable() >> true
            _ * getId() >> "id"
            1 * getIngressReader() >> Mock(Reader) {
                1 * read(new JsonRpcRequest("eth_sendRawTransaction", new ListParams(["0x1"]))) >>
                        Mono.error(new JsonRpcException(1, "too low"))
            }
        }
        def up2 = Mock(Upstream) {
            1 * isAvailable() >> true
            _ * getId() >> "id"
            1 * getIngressReader() >> Mock(Reader) {
                1 * read(new JsonRpcRequest("eth_sendRawTransaction", new ListParams(["0x1"]))) >>
                        Mono.error(new JsonRpcException(1, "too low"))
            }
        }
        def reader = new BroadcastReader([up, up1, up2], new Selector.EmptyMatcher(), null, new BroadcastQuorum(), Stub(Tracer))
        when:
        def act = reader.read(new JsonRpcRequest("eth_sendRawTransaction", new ListParams(["0x1"])))
        then:
        StepVerifier.create(act)
                .expectError(JsonRpcException.class)
                .verify(Duration.ofSeconds(3))
    }

    def "No response if no available upstreams"() {
        setup:
        def up = Mock(Upstream) {
            1 * isAvailable() >> false
            0 * getId() >> "id"
            0 * getIngressReader() >> Mock(Reader)
        }
        def up1 = Mock(Upstream) {
            1 * isAvailable() >> false
            0 * getId() >> "id"
            0 * getIngressReader() >> Mock(Reader)
        }
        def up2 = Mock(Upstream) {
            1 * isAvailable() >> false
            0 * getId() >> "id"
            0 * getIngressReader() >> Mock(Reader)
        }
        def reader = new BroadcastReader([up, up1, up2], new Selector.EmptyMatcher(), null, new BroadcastQuorum(), Stub(Tracer))
        when:
        def act = reader
                .read(new JsonRpcRequest("eth_sendRawTransaction", new ListParams(["0x1"])))
                .switchIfEmpty(Mono.just(new RpcReader.Result(new byte[0], null, 0, null, null)))
        then:
        StepVerifier.create(act)
                .expectErrorMessage("Unhandled Upstream error")
                .verify(Duration.ofSeconds(3))
    }
}
