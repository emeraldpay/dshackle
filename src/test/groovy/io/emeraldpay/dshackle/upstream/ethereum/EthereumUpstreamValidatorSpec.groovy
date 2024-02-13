/**
 * Copyright (c) 2022 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.upstream.ethereum


import io.emeraldpay.dshackle.foundation.ChainOptions
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.test.ApiReaderMock
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcError
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.ethereum.domain.Address
import io.emeraldpay.etherjar.hex.HexData
import io.emeraldpay.etherjar.rpc.RpcResponseError
import io.emeraldpay.dshackle.upstream.ethereum.json.TransactionCallJson
import reactor.core.publisher.Mono
import reactor.util.function.Tuples
import spock.lang.Specification

import java.time.Duration

import static io.emeraldpay.dshackle.Chain.ETHEREUM__MAINNET
import static io.emeraldpay.dshackle.Chain.OPTIMISM__MAINNET
import static io.emeraldpay.dshackle.upstream.UpstreamAvailability.*
import static io.emeraldpay.dshackle.upstream.ValidateUpstreamSettingsResult.UPSTREAM_FATAL_SETTINGS_ERROR
import static io.emeraldpay.dshackle.upstream.ValidateUpstreamSettingsResult.UPSTREAM_SETTINGS_ERROR
import static io.emeraldpay.dshackle.upstream.ValidateUpstreamSettingsResult.UPSTREAM_VALID
import static java.util.Collections.emptyList
import io.emeraldpay.dshackle.config.ChainsConfig.ChainConfig

class EthereumUpstreamValidatorSpec extends Specification {

    def conf = ChainConfig.defaultWithContract("0x32268860cAAc2948Ab5DdC7b20db5a420467Cf96")

    def "Resolve to final availability"() {
        setup:

        def validator = new EthereumUpstreamValidator(ETHEREUM__MAINNET, Stub(Upstream), ChainOptions.PartialOptions.getDefaults().buildOptions(), conf)
        expect:
        validator.resolve(Tuples.of(sync, peers)) == exp
        where:
        exp         | sync          | peers
        OK          | OK            | OK
        IMMATURE    | OK            | IMMATURE
        UNAVAILABLE | OK            | UNAVAILABLE
        SYNCING     | SYNCING       | OK
        SYNCING     | SYNCING       | IMMATURE
        UNAVAILABLE | SYNCING       | UNAVAILABLE
        UNAVAILABLE | UNAVAILABLE   | OK
        UNAVAILABLE | UNAVAILABLE   | IMMATURE
        UNAVAILABLE | UNAVAILABLE   | UNAVAILABLE
    }

    def "Doesnt check eth_syncing when disabled"() {
        setup:
        def options = ChainOptions.PartialOptions.getDefaults().tap {
            it.validateSyncing = false
        }.buildOptions()
        def up = Mock(Upstream)
        def validator = new EthereumUpstreamValidator(ETHEREUM__MAINNET, up, options, conf)

        when:
        def act = validator.validateSyncing().block(Duration.ofSeconds(1))
        then:
        act == OK
        0 * up.getIngressReader()
    }

    def "Syncing is OK when false returned from upstream"() {
        setup:
        def options = ChainOptions.PartialOptions.getDefaults().tap {
            it.validateSyncing = true
        }.buildOptions()
        def up = TestingCommons.upstream(
                new ApiReaderMock().tap {
                    answer("eth_syncing", [], false)
                }
        )
        def validator = new EthereumUpstreamValidator(ETHEREUM__MAINNET, up, options, conf)

        when:
        def act = validator.validateSyncing().block(Duration.ofSeconds(1))
        then:
        act == OK
    }

    def "Execute onSyncingNode with result of eth_syncing"() {
        setup:
        def options = ChainOptions.PartialOptions.getDefaults().tap {
            it.validateSyncing = true
        }.buildOptions()
        def up = Mock(Upstream) {
            2 * getIngressReader() >> Mock(Reader) { reader ->
                2 * reader.read(_) >>> [
                        Mono.just(new JsonRpcResponse('true'.getBytes(), null)),
                        Mono.just(new JsonRpcResponse('false'.getBytes(), null))
                ]
            }
            2 * getHead() >> Mock(Head) {head ->
                1 * head.onSyncingNode(true)
                1 * head.onSyncingNode(false)
            }
        }
        def validator = new EthereumUpstreamValidator(ETHEREUM__MAINNET, up, options, conf)

        when:
        def act = validator.validateSyncing().block(Duration.ofSeconds(1))
        def act2 = validator.validateSyncing().block(Duration.ofSeconds(1))
        then:
        act == SYNCING
        act2 == OK
    }

    def "Syncing is SYNCING when state returned from upstream"() {
        setup:
        def options = ChainOptions.PartialOptions.getDefaults().tap {
            it.validateSyncing = true
        }.buildOptions()
        def up = TestingCommons.upstream(
                new ApiReaderMock().tap {
                    answer("eth_syncing", [], [startingBlock: 100, currentBlock: 50])
                }
        )
        def validator = new EthereumUpstreamValidator(ETHEREUM__MAINNET, up, options, conf)

        when:
        def act = validator.validateSyncing().block(Duration.ofSeconds(1))
        then:
        act == SYNCING
    }

    def "Syncing is UNAVAILABLE when error returned from upstream"() {
        setup:
        def options = ChainOptions.PartialOptions.getDefaults().tap {
            it.validateSyncing = true
        }.buildOptions()
        def up = TestingCommons.upstream(
                new ApiReaderMock().tap {
                    answer("eth_syncing", [], new RpcResponseError(RpcResponseError.CODE_METHOD_NOT_EXIST, "Unavailable"))
                }
        )
        def validator = new EthereumUpstreamValidator(ETHEREUM__MAINNET, up, options, conf)

        when:
        def act = validator.validateSyncing().block(Duration.ofSeconds(1))
        then:
        act == UNAVAILABLE
    }

    def "Doesnt validate peers when disabled"() {
        setup:
        def options = ChainOptions.PartialOptions.getDefaults().tap {
            it.validatePeers = false
            it.minPeers = 10
        }.buildOptions()
        def up = Mock(Upstream)
        def validator = new EthereumUpstreamValidator(ETHEREUM__MAINNET, up, options, conf)

        when:
        def act = validator.validatePeers().block(Duration.ofSeconds(1))
        then:
        act == OK
        0 * up.getApi()
    }

    def "Doesnt validate peers when zero peers is expected"() {
        setup:
        def options = ChainOptions.PartialOptions.getDefaults().tap {
            it.validatePeers = true
            it.minPeers = 0
        }.buildOptions()
        def up = Mock(Upstream)
        def validator = new EthereumUpstreamValidator(ETHEREUM__MAINNET, up, options, conf)

        when:
        def act = validator.validatePeers().block(Duration.ofSeconds(1))
        then:
        act == OK
        0 * up.getIngressReader()
    }

    def "Peers is IMMATURE when state returned too few peers"() {
        setup:
        def options = ChainOptions.PartialOptions.getDefaults().tap {
            it.validatePeers = true
            it.minPeers = 10
        }.buildOptions()
        def up = TestingCommons.upstream(
                new ApiReaderMock().tap {
                    answer("net_peerCount", [], "0x5")
                }
        )
        def validator = new EthereumUpstreamValidator(ETHEREUM__MAINNET, up, options, conf)

        when:
        def act = validator.validatePeers().block(Duration.ofSeconds(1))
        then:
        act == IMMATURE
    }

    def "Peers is OK when state returned exactly min peers"() {
        setup:
        def options = ChainOptions.PartialOptions.getDefaults().tap {
            it.validatePeers = true
            it.minPeers = 10
        }.buildOptions()
        def up = TestingCommons.upstream(
                new ApiReaderMock().tap {
                    answer("net_peerCount", [], "0xa")
                }
        )
        def validator = new EthereumUpstreamValidator(ETHEREUM__MAINNET, up, options, conf)

        when:
        def act = validator.validatePeers().block(Duration.ofSeconds(1))
        then:
        act == OK
    }

    def "Peers is OK when state returned more than enough peers"() {
        setup:
        def options = ChainOptions.PartialOptions.getDefaults().tap {
            it.validatePeers = true
            it.minPeers = 10
        }.buildOptions()
        def up = TestingCommons.upstream(
                new ApiReaderMock().tap {
                    answer("net_peerCount", [], "0xff")
                }
        )
        def validator = new EthereumUpstreamValidator(ETHEREUM__MAINNET, up, options, conf)

        when:
        def act = validator.validatePeers().block(Duration.ofSeconds(1))
        then:
        act == OK
    }

    def "Peers is UNAVAILABLE when state returned error"() {
        setup:
        def options = ChainOptions.PartialOptions.getDefaults().tap {
            it.validatePeers = true
            it.minPeers = 10
        }.buildOptions()
        def up = TestingCommons.upstream(
                new ApiReaderMock().tap {
                    answer("net_peerCount", [], new RpcResponseError(RpcResponseError.CODE_METHOD_NOT_EXIST, "Unavailable"))
                }
        )
        def validator = new EthereumUpstreamValidator(ETHEREUM__MAINNET, up, options, conf)

        when:
        def act = validator.validatePeers().block(Duration.ofSeconds(1))
        then:
        act == UNAVAILABLE
    }

    def "Doesnt validate chan and callLimit when disabled"() {
        setup:
        def options = ChainOptions.PartialOptions.getDefaults().tap {
            it.validateCalllimit = false
            it.validateChain = false
        }.buildOptions()
        def up = Mock(Upstream) {
            2 * getIngressReader() >>
                    Mock(Reader) {
                        1 * read(new JsonRpcRequest("eth_blockNumber", [])) >> Mono.just(new JsonRpcResponse('"0x10ff9be"'.getBytes(), null))
                        1 * read(new JsonRpcRequest("eth_getBlockByNumber", ["0x10fd2ae", false])) >>
                                Mono.just(new JsonRpcResponse('"result"'.getBytes(), null))
                    }
        }
        def validator = new EthereumUpstreamValidator(ETHEREUM__MAINNET, up, options, conf)

        when:
        def act = validator.validateUpstreamSettingsOnStartup()
        then:
        act == UPSTREAM_VALID
    }

    def "Upstream is valid if not error from call limit check"() {
        setup:
        def options = ChainOptions.PartialOptions.getDefaults().tap {
            it.validateChain = false
        }.buildOptions()
        def up = Mock(Upstream) {
            3 * getIngressReader() >> Mock(Reader) {
                1 * read(new JsonRpcRequest("eth_call", [new TransactionCallJson(
                        Address.from("0x32268860cAAc2948Ab5DdC7b20db5a420467Cf96"),
                        HexData.from("0xd8a26e3a00000000000000000000000000000000000000000000000000000000000f4240")
                ), "latest"])) >> Mono.just(new JsonRpcResponse("0x00000000000000000000".getBytes(), null))
                1 * read(new JsonRpcRequest("eth_blockNumber", [])) >> Mono.just(new JsonRpcResponse('"0x10ff9be"'.getBytes(), null))
                1 * read(new JsonRpcRequest("eth_getBlockByNumber", ["0x10fd2ae", false])) >>
                        Mono.just(new JsonRpcResponse('"result"'.getBytes(), null))
            }
        }
        def validator = new EthereumUpstreamValidator(ETHEREUM__MAINNET, up, options, conf)
//"0x32268860cAAc2948Ab5DdC7b20db5a420467Cf96
        when:
        def act = validator.validateUpstreamSettingsOnStartup()
        then:
        act == UPSTREAM_VALID
    }

    def "Upstream is not valid if error returned on call limit check"() {
        setup:
        def options = ChainOptions.PartialOptions.getDefaults().tap {
            it.validateChain = false
        }.buildOptions()
        def up = Mock(Upstream) {
            3 * getIngressReader() >> Mock(Reader) {
                1 * read(new JsonRpcRequest("eth_call", [new TransactionCallJson(
                        Address.from("0x32268860cAAc2948Ab5DdC7b20db5a420467Cf96"),
                        HexData.from("0xd8a26e3a00000000000000000000000000000000000000000000000000000000000f4240")
                ), "latest"])) >> Mono.just(new JsonRpcResponse(null, new JsonRpcError(1, "Too long")))
                1 * read(new JsonRpcRequest("eth_blockNumber", [])) >> Mono.just(new JsonRpcResponse('"0x10ff9be"'.getBytes(), null))
                1 * read(new JsonRpcRequest("eth_getBlockByNumber", ["0x10fd2ae", false])) >>
                        Mono.just(new JsonRpcResponse('"result"'.getBytes(), null))
            }
        }
        def validator = new EthereumUpstreamValidator(ETHEREUM__MAINNET, up, options, conf)
// "0x32268860cAAc2948Ab5DdC7b20db5a420467Cf96"
        when:
        def act = validator.validateUpstreamSettingsOnStartup()
        then:
        act == UPSTREAM_SETTINGS_ERROR
    }

    def "Upstream is valid if chain settings are valid"() {
        setup:
        def options = ChainOptions.PartialOptions.getDefaults().tap {
            it.validateCalllimit = false
        }.buildOptions()
        def up = Mock(Upstream) {
            4 * getIngressReader() >> Mock(Reader) {
                1 * read(new JsonRpcRequest("eth_chainId", emptyList())) >> Mono.just(new JsonRpcResponse('"0x1"'.getBytes(), null))
                1 * read(new JsonRpcRequest("net_version", emptyList())) >> Mono.just(new JsonRpcResponse('"1"'.getBytes(), null))
                1 * read(new JsonRpcRequest("eth_blockNumber", [])) >> Mono.just(new JsonRpcResponse('"0x10ff9be"'.getBytes(), null))
                1 * read(new JsonRpcRequest("eth_getBlockByNumber", ["0x10fd2ae", false])) >>
                        Mono.just(new JsonRpcResponse('"result"'.getBytes(), null))
            }
        }
        def validator = new EthereumUpstreamValidator(ETHEREUM__MAINNET, up, options, conf)
// "0x32268860cAAc2948Ab5DdC7b20db5a420467Cf96"
        when:
        def act = validator.validateUpstreamSettingsOnStartup()
        then:
        act == UPSTREAM_VALID
    }

    def "Upstream is not valid - specified optimism but got ethereum"() {
        setup:
        def options = ChainOptions.PartialOptions.getDefaults().tap {
            it.validateCalllimit = false
        }.buildOptions()
        def up = Mock(Upstream) {
            4 * getIngressReader() >> Mock(Reader) {
                1 * read(new JsonRpcRequest("eth_chainId", emptyList())) >> Mono.just(new JsonRpcResponse('"0x1"'.getBytes(), null))
                1 * read(new JsonRpcRequest("net_version", emptyList())) >> Mono.just(new JsonRpcResponse('"1"'.getBytes(), null))
                1 * read(new JsonRpcRequest("eth_blockNumber", [])) >> Mono.just(new JsonRpcResponse('"0x10ff9be"'.getBytes(), null))
                1 * read(new JsonRpcRequest("eth_getBlockByNumber", ["0x10fd2ae", false])) >>
                        Mono.just(new JsonRpcResponse('"result"'.getBytes(), null))
            }
        }
        def validator = new EthereumUpstreamValidator(OPTIMISM__MAINNET, up, options, conf)
// "0x32268860cAAc2948Ab5DdC7b20db5a420467Cf96"
        when:
        def act = validator.validateUpstreamSettingsOnStartup()
        then:
        act == UPSTREAM_FATAL_SETTINGS_ERROR
    }

    def "Upstream is valid if all setting are valid"() {
        setup:
        def options = ChainOptions.PartialOptions.getDefaults().buildOptions()
        def up = Mock(Upstream) {
            5 * getIngressReader() >> Mock(Reader) {
                1 * read(new JsonRpcRequest("eth_chainId", emptyList())) >> Mono.just(new JsonRpcResponse('"0x1"'.getBytes(), null))
                1 * read(new JsonRpcRequest("net_version", emptyList())) >> Mono.just(new JsonRpcResponse('"1"'.getBytes(), null))
                1 * read(new JsonRpcRequest("eth_call", [new TransactionCallJson(
                        Address.from("0x32268860cAAc2948Ab5DdC7b20db5a420467Cf96"),
                        HexData.from("0xd8a26e3a00000000000000000000000000000000000000000000000000000000000f4240")
                ), "latest"])) >> Mono.just(new JsonRpcResponse("0x00000000000000000000".getBytes(), null))
                1 * read(new JsonRpcRequest("eth_blockNumber", [])) >> Mono.just(new JsonRpcResponse('"0x10ff9be"'.getBytes(), null))
                1 * read(new JsonRpcRequest("eth_getBlockByNumber", ["0x10fd2ae", false])) >>
                        Mono.just(new JsonRpcResponse('"result"'.getBytes(), null))
            }
        }
        def validator = new EthereumUpstreamValidator(ETHEREUM__MAINNET, up, options, conf)
// "0x32268860cAAc2948Ab5DdC7b20db5a420467Cf96"
        when:
        def act = validator.validateUpstreamSettingsOnStartup()
        then:
        act == UPSTREAM_VALID
    }

    def "Upstream is not valid if there are errors"() {
        setup:
        def options = ChainOptions.PartialOptions.getDefaults().buildOptions()
        def up = Mock(Upstream) {
            5 * getIngressReader() >> Mock(Reader) {
                1 * read(new JsonRpcRequest("eth_chainId", emptyList())) >> Mono.just(new JsonRpcResponse(null, new JsonRpcError(1, "Too long")))
                1 * read(new JsonRpcRequest("net_version", emptyList())) >> Mono.just(new JsonRpcResponse(null, new JsonRpcError(1, "Too long")))
                1 * read(new JsonRpcRequest("eth_call", [new TransactionCallJson(
                        Address.from("0x32268860cAAc2948Ab5DdC7b20db5a420467Cf96"),
                        HexData.from("0xd8a26e3a00000000000000000000000000000000000000000000000000000000000f4240")
                ), "latest"])) >> Mono.just(new JsonRpcResponse(null, new JsonRpcError(1, "Too long")))
                1 * read(new JsonRpcRequest("eth_blockNumber", [])) >> Mono.just(new JsonRpcResponse('"0x10ff9be"'.getBytes(), null))
                1 * read(new JsonRpcRequest("eth_getBlockByNumber", ["0x10fd2ae", false])) >>
                        Mono.just(new JsonRpcResponse('"result"'.getBytes(), null))
            }
        }
        def validator = new EthereumUpstreamValidator(ETHEREUM__MAINNET, up, options, conf)
// "0x32268860cAAc2948Ab5DdC7b20db5a420467Cf96"
        when:
        def act = validator.validateUpstreamSettingsOnStartup()
        then:
        act == UPSTREAM_SETTINGS_ERROR
    }


}
