package io.emeraldpay.dshackle.upstream.rpcclient

import io.emeraldpay.dshackle.reader.Reader
import reactor.core.publisher.Mono
import spock.lang.Specification

import java.time.Duration

class JsonRpcSwitchClientSpec extends Specification {

    def "Uses primary response if it works"() {
        setup:
        def primaryCalled = false
        def secondaryCalled = false
        def request = new JsonRpcRequest("eth_test", [])
        def response = JsonRpcResponse.ok("test".bytes, new JsonRpcResponse.NumberId(100))
        def primary = Mock(Reader<JsonRpcRequest, JsonRpcResponse>) {
            1 * read(request) >> Mono.fromCallable {
                primaryCalled = true
                response
            }
        }
        def secondary = Mock(Reader<JsonRpcRequest, JsonRpcResponse>) {
            _ * read(request) >> Mono.fromCallable {
                secondaryCalled = true
                response
            }
        }

        def client = new JsonRpcSwitchClient(primary, secondary)

        when:
        def act = client.read(request).block(Duration.ofSeconds(1))

        then:
        act == response
        primaryCalled
        !secondaryCalled
    }

    def "Uses secondary response if primary fails"() {
        setup:
        def primaryCalled = false
        def secondaryCalled = false
        def request = new JsonRpcRequest("eth_test", [])
        def response = JsonRpcResponse.ok("test".bytes, new JsonRpcResponse.NumberId(100))
        def primary = Mock(Reader<JsonRpcRequest, JsonRpcResponse>) {
            1 * read(request) >> Mono.fromCallable {
                primaryCalled = true
                throw new IllegalStateException("Primary Fail")
            }
        }
        def secondary = Mock(Reader<JsonRpcRequest, JsonRpcResponse>) {
            1 * read(request) >> Mono.fromCallable {
                secondaryCalled = true
                response
            }
        }

        def client = new JsonRpcSwitchClient(primary, secondary)

        when:
        def act = client.read(request).block(Duration.ofSeconds(1))

        then:
        act == response
        primaryCalled
        secondaryCalled
    }
}
