package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import reactor.core.publisher.Mono
import spock.lang.Specification

import java.time.Duration

class RequestPostprocessorSpec extends Specification {

    def "Wrappers calls onReceive for a value"() {
        setup:
        def request = new JsonRpcRequest("test_foo", [1], 1, null)
        def processor = Mock(RequestPostprocessor)
        def api = TestingCommons.api()
        api.answer("test_foo", [1], "test")
        def wrapped = new RequestPostprocessor.Wrapper(api, processor)

        when:
        def act = wrapped.read(request).block(Duration.ofSeconds(1))

        then:
        act.hasResult()
        act.resultAsProcessedString == "test"
        1 * processor.onReceive("test_foo", [1], "\"test\"".bytes)
    }

    def "Wrappers doesn't call onReceive for no value"() {
        setup:
        def request = new JsonRpcRequest("test_foo", [1], 1, null)
        def processor = Mock(RequestPostprocessor)
        Reader<JsonRpcRequest, JsonRpcResponse> reader = Mock(Reader) {
            1 * it.read(request) >> Mono.empty()
        }
        def wrapped = new RequestPostprocessor.Wrapper(reader, processor)

        when:
        def act = wrapped.read(request).block(Duration.ofSeconds(1))

        then:
        act == null
        0 * processor.onReceive(_, _, _)
    }
}
