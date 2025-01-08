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
                                            "code": "0x6080604052348015600f57600080fd5b506004361060285760003560e01c806351be4eaa14602d575b600080fd5b60336047565b604051603e91906066565b60405180910390f35b60005a905090565b6000819050919050565b606081604f565b82525050565b6000602082019050607960008301846059565b9291505056fea26469706673582212201c0202887c1afe66974b06ee355dee07542bbc424cf4d1659c91f56c08c3dcc064736f6c63430008130033",
                                    ],
                            ]], gas)
                }
        )
        def detector = new EthereumUpstreamSettingsDetector(up, Chain.ETHEREUM__MAINNET)

        when:
        def act = detector.internalDetectLabels()
        then:
        def result = StepVerifier.create(act)
            .expectNext(
                    new Pair<String, String>("client_type", clientType),
                    new Pair<String, String>("client_version", version),
                    new Pair<String, String>("archive", "true"),
                    new Pair<String, String>("gas-limit", gasv),
            )
        if (extragas != null) {
            result.expectNext(new Pair<String, String>("extra_gas_limit", extragas))
        }
        result.expectComplete().verify(Duration.ofSeconds(1))
        where:
        response                                              | gas         | gasv        | extragas    | clientType      |  version
        "Nethermind/v1.19.3+e8ac1da4/linux-x64/dotnet7.0.8"   | "0x2fa9dc2" | "50000000"  | "50000000"  | "nethermind"    |  "v1.19.3+e8ac1da4"
        "Geth/v1.12.0-stable-e501b3b0/linux-amd64/go1.20.3"   | "0x2fa9dc2" | "50000000"  | "50000000"  | "geth"          |  "v1.12.0-stable-e501b3b0"
        "Erigon/v1.12.0-stable-e501b3b0/linux-amd64/go1.20.3" | "0x2fa9dc2" | "50000000"  | "50000000"  | "erigon"        |  "v1.12.0-stable-e501b3b0"
        "Bor/v0.4.0/linux-amd64/go1.19.10"                    | "0x23c2f342"| "600000000" | "600000000" | "bor"           |  "v0.4.0"
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
                                        "code": "0x6080604052348015600f57600080fd5b506004361060285760003560e01c806351be4eaa14602d575b600080fd5b60336047565b604051603e91906066565b60405180910390f35b60005a905090565b6000819050919050565b606081604f565b82525050565b6000602082019050607960008301846059565b9291505056fea26469706673582212201c0202887c1afe66974b06ee355dee07542bbc424cf4d1659c91f56c08c3dcc064736f6c63430008130033",
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
