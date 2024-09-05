package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.test.ApiReaderMock
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.ChainResponse
import io.emeraldpay.dshackle.upstream.DefaultUpstream
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
                    answer("eth_call", [[
                            "to": "0x53Daa71B04d589429f6d3DF52db123913B818F22",
                            "data": "0x51be4eaa",
                    ],
                            "latest",
                            [
                                    "0x53Daa71B04d589429f6d3DF52db123913B818F22": [
                                            "code": "0x6080604052348015600e575f80fd5b50600436106026575f3560e01c806351be4eaa14602a575b5f80fd5b60306044565b604051603b91906061565b60405180910390f35b5f5a905090565b5f819050919050565b605b81604b565b82525050565b5f60208201905060725f8301846054565b9291505056fea2646970667358221220a85b088da3911ea743505594ac7cfdd1a65865de64499ee1f3c6bd9cdad4552364736f6c634300081a0033",
                                    ],
                            ]], "0x2fa9dc4")
                }
        )
        def detector = new EthereumUpstreamSettingsDetector(up, Chain.ETHEREUM__MAINNET)

        when:
        def act = detector.internalDetectLabels()
        then:
        StepVerifier.create(act)
            .expectNext(
                    new Pair<String, String>("client_type", clientType),
                    new Pair<String, String>("client_version", version),
                    new Pair<String, String>("archive", "true"),
                    new Pair<String, String>("gas-limit", "50000000")
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

    def "Not archival node if null response"() {
        setup:
        def up = TestingCommons.upstream(
                new ApiReaderMock().tap {
                    answer("web3_clientVersion", [], "Bor/v0.4.0/linux-amd64/go1.19.10")
                    answer("eth_blockNumber", [], "0x10df3e5")
                    answer("eth_getBalance", ["0x0000000000000000000000000000000000000000", "0x10dccd5"], "")
                    answer("eth_getBalance", ["0x0000000000000000000000000000000000000000", "0x2710"], null)
                }
        )
        def detector = new EthereumUpstreamSettingsDetector(up, Chain.ETHEREUM__MAINNET)

        when:
        def act = detector.internalDetectLabels()
        then:
        StepVerifier.create(act)
                .expectNext(
                        new Pair<String, String>("client_type", "bor"),
                        new Pair<String, String>("client_version", "v0.4.0"),
                        new Pair<String, String>("archive", "false")
                )
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "No archive label if it is set manually"() {
        setup:
        def up = TestingCommons.upstream(
                new ApiReaderMock().tap {
                    answer("web3_clientVersion", [], "Bor/v0.4.0/linux-amd64/go1.19.10")
                    answer("eth_blockNumber", [], "0x10df3e5")
                },
                Map.of("archive", "false")
        )
        def detector = new EthereumUpstreamSettingsDetector(up, Chain.ETHEREUM__MAINNET)

        when:
        def act = detector.internalDetectLabels()
        then:
        StepVerifier.create(act)
                .expectNext(
                        new Pair<String, String>("client_type", "bor"),
                        new Pair<String, String>("client_version", "v0.4.0"),
                )
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Only default label"() {
        setup:
        def up = Mock(DefaultUpstream) {
            5 * getIngressReader() >> Mock(Reader) {
                1 * read(new ChainRequest("web3_clientVersion", new ListParams())) >>
                        Mono.just(new ChainResponse('no/v1.19.3+e8ac1da4/linux-x64/dotnet7.0.8'.getBytes(), null))
                1 * read(new ChainRequest("eth_blockNumber", new ListParams())) >>
                        Mono.just(new ChainResponse("\"0x10df3e5\"".getBytes(), null))
                1 * read(new ChainRequest("eth_getBalance", new ListParams(["0x0000000000000000000000000000000000000000", "0x10dccd5"]))) >>
                        Mono.error(new RuntimeException())
                1 * read(new ChainRequest("eth_getBalance", new ListParams(["0x0000000000000000000000000000000000000000", "0x2710"]))) >>
                        Mono.just(new ChainResponse("".getBytes(), null))
                1 * read(new ChainRequest("eth_call", new ListParams([
                                "to": "0x53Daa71B04d589429f6d3DF52db123913B818F22",
                                "data": "0x51be4eaa",
                        ],
                        "latest",
                        [
                                "0x53Daa71B04d589429f6d3DF52db123913B818F22": [
                                        "code": "0x6080604052348015600e575f80fd5b50600436106026575f3560e01c806351be4eaa14602a575b5f80fd5b60306044565b604051603b91906061565b60405180910390f35b5f5a905090565b5f819050919050565b605b81604b565b82525050565b5f60208201905060725f8301846054565b9291505056fea2646970667358221220a85b088da3911ea743505594ac7cfdd1a65865de64499ee1f3c6bd9cdad4552364736f6c634300081a0033",
                                ],
                        ],
                ))) >>
                        Mono.just(new ChainResponse("".getBytes(), null))
            }
            getLabels() >> []
        }
        def detector = new EthereumUpstreamSettingsDetector(up, Chain.ETHEREUM__MAINNET)
        when:
        def act = detector.internalDetectLabels()
        then:
        StepVerifier.create(act)
            .expectNext(new Pair<String, String>("archive", "false"))
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
            1 * getLabels() >> List.of()
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
