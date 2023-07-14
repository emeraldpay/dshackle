package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.test.ApiReaderMock
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.EthereumLabelsDetector
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import kotlin.Pair
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Duration

class EthereumLabelsDetectorSpec extends Specification {

    def "Detect labels"() {
        setup:
        def up = TestingCommons.upstream(
                new ApiReaderMock().tap {
                    answer("web3_clientVersion", [], response)
                    answer("eth_blockNumber", [], "0x10df3e5")
                    answer("eth_getBalance", ["0x756F45E3FA69347A9A973A725E3C98bC4db0b5a0", "0x10dccd5"], "")
                }
        )
        def detector = new EthereumLabelsDetector(up.getIngressReader())

        when:
        def act = detector.detectLabels()
        then:
        StepVerifier.create(act)
            .expectNext(
                    new Pair<String, String>("client_type", clientType),
                    new Pair<String, String>("archive", "true")
            )
            .expectComplete()
            .verify(Duration.ofSeconds(1))
        where:
        response                                                | clientType
        "Nethermind/v1.19.3+e8ac1da4/linux-x64/dotnet7.0.8"     | "nethermind"
        "Geth/v1.12.0-stable-e501b3b0/linux-amd64/go1.20.3"     | "geth"
        "Erigon/v1.12.0-stable-e501b3b0/linux-amd64/go1.20.3"   | "erigon"
        "Bor/v0.4.0/linux-amd64/go1.19.10"                      | "bor"
    }

    def "No any label"() {
        setup:
        def up = Mock(DefaultUpstream) {
            1 * getIngressReader() >> Mock(Reader) {
                1 * read(new JsonRpcRequest("web3_clientVersion", [])) >>
                        Mono.just(new JsonRpcResponse('no/v1.19.3+e8ac1da4/linux-x64/dotnet7.0.8'.getBytes(), null))
                1 * read(new JsonRpcRequest("eth_blockNumber", [])) >>
                        Mono.just(new JsonRpcResponse("\"0x10df3e5\"".getBytes(), null))
                1 * read(new JsonRpcRequest("eth_getBalance", ["0x756F45E3FA69347A9A973A725E3C98bC4db0b5a0", "0x10dccd5"])) >>
                        Mono.error(new RuntimeException())
            }
        }
        def detector = new EthereumLabelsDetector(up.getIngressReader())
        when:
        def act = detector.detectLabels()
        then:
        StepVerifier.create(act)
            .expectComplete()
            .verify(Duration.ofSeconds(1))
    }
}
