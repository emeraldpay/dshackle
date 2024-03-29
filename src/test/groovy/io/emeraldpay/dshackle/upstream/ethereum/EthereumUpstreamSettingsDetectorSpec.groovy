package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.test.ApiReaderMock
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.ChainResponse
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import kotlin.Pair
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Duration

class EthereumUpstreamSettingsDetectorSpec extends Specification {

    def "Detect labels"() {
        setup:
        def up = TestingCommons.upstream(
                new ApiReaderMock().tap {
                    answer("web3_clientVersion", [], response)
                    answer("eth_blockNumber", [], "0x10df3e5")
                    answer("eth_getBalance", ["0x0000000000000000000000000000000000000000", "0x10dccd5"], "")
                    answer("eth_getBalance", ["0x0000000000000000000000000000000000000000", "0x2710"], "")
                }
        )
        def detector = new EthereumUpstreamSettingsDetector(up, Chain.ETHEREUM__MAINNET)

        when:
        def act = detector.detectLabels()
        then:
        StepVerifier.create(act)
            .expectNext(
                    new Pair<String, String>("client_type", clientType),
                    new Pair<String, String>("client_version", version),
                    new Pair<String, String>("archive", "true")
            )
            .expectComplete()
            .verify(Duration.ofSeconds(1))
        where:
        response                                                | clientType      |  version
        "Nethermind/v1.19.3+e8ac1da4/linux-x64/dotnet7.0.8"     | "nethermind"    |  "v1.19.3+e8ac1da4"
        "Geth/v1.12.0-stable-e501b3b0/linux-amd64/go1.20.3"     | "geth"          |  "v1.12.0-stable-e501b3b0"
        "Erigon/v1.12.0-stable-e501b3b0/linux-amd64/go1.20.3"   | "erigon"        |  "v1.12.0-stable-e501b3b0"
        "Bor/v0.4.0/linux-amd64/go1.19.10"                      | "bor"           |  "v0.4.0"
    }

    def "No any label"() {
        setup:
        def up = Mock(DefaultUpstream) {
            4 * getIngressReader() >> Mock(Reader) {
                1 * read(new ChainRequest("web3_clientVersion", new ListParams())) >>
                        Mono.just(new ChainResponse('no/v1.19.3+e8ac1da4/linux-x64/dotnet7.0.8'.getBytes(), null))
                1 * read(new ChainRequest("eth_blockNumber", new ListParams())) >>
                        Mono.just(new ChainResponse("\"0x10df3e5\"".getBytes(), null))
                1 * read(new ChainRequest("eth_getBalance", new ListParams(["0x0000000000000000000000000000000000000000", "0x10dccd5"]))) >>
                        Mono.error(new RuntimeException())
                1 * read(new ChainRequest("eth_getBalance", new ListParams(["0x0000000000000000000000000000000000000000", "0x2710"]))) >>
                        Mono.just(new ChainResponse("".getBytes(), null))
            }
        }
        def detector = new EthereumUpstreamSettingsDetector(up, Chain.ETHEREUM__MAINNET)
        when:
        def act = detector.detectLabels()
        then:
        StepVerifier.create(act)
            .expectComplete()
            .verify(Duration.ofSeconds(1))
    }

    def "Detect client version"() {
        setup:
        def up = Mock(DefaultUpstream) {
            2 * getIngressReader() >> Mock(Reader) {
                1 * read(new ChainRequest("web3_clientVersion", new ListParams())) >>
                        Mono.just(new ChainResponse('"Erigon/v1.12.0-stable-e501b3b0/linux-amd64/go1.20.3"'.getBytes(), null))
            }
        }
        def detector = new EthereumUpstreamSettingsDetector(up, Chain.ETHEREUM__MAINNET)
        when:
        def act = detector.detectClientVersion()
        then:
        StepVerifier.create(act)
            .expectNext("Erigon/v1.12.0-stable-e501b3b0/linux-amd64/go1.20.3")
            .expectComplete()
            .verify(Duration.ofSeconds(1))
    }
}
