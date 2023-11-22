package io.emeraldpay.dshackle.upstream.calls

import io.emeraldpay.dshackle.Chain
import spock.lang.Specification

class DefaultEthereumMethodsSpec extends Specification {

    def "eth_chainId is available"() {
        setup:
        def methods = new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET, false)
        when:
        def act = methods.isAvailable("eth_chainId")
        then:
        act
    }

    def "eth_chainId is hardcoded"() {
        setup:
        def methods = new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET, false)
        when:
        def act = methods.isHardcoded("eth_chainId")
        then:
        act
    }

    def "eth_chainId is not callable"() {
        setup:
        def methods = new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET, false)
        when:
        def act = methods.isCallable("eth_chainId")
        then:
        !act
    }

    def "Provides hardcoded correct chainId"() {
        expect:
        new String(new DefaultEthereumMethods(chain, false).executeHardcoded("eth_chainId")) == id
        where:
        chain                  | id
        Chain.ETHEREUM__MAINNET | '"0x1"'
        Chain.ETHEREUM_CLASSIC__MAINNET | '"0x3d"'
        Chain.ETHEREUM__GOERLI | '"0x5"'
    }

    def "Optimism chain unsupported methods"() {
        setup:
        def methods = new DefaultEthereumMethods(Chain.OPTIMISM__MAINNET, false)
        when:
        def acc = methods.isAvailable("eth_getAccounts")
        def trans = methods.isAvailable("eth_sendTransaction")
        then:
        !acc
        !trans
    }

    def "Has supported specific methods"() {
        expect:
        new DefaultEthereumMethods(chain, false).getSupportedMethods().containsAll(methods)
        where:
        chain          | methods
        Chain.POLYGON__MAINNET | ["bor_getAuthor",
                                      "bor_getCurrentValidators",
                                      "bor_getCurrentProposer",
                                      "bor_getRootHash",
                                      "bor_getSignersAtHash",
                                      "eth_getRootHash"]
        Chain.OPTIMISM__MAINNET | ["rollup_gasPrices"]
    }

    def "Has no filter methods by default"() {
        setup:
        def methods = new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET, false)
        when:
        def act = methods.getSupportedMethods().findAll { it.containsIgnoreCase("filter") }
        then:
        act.isEmpty()
    }

    def "Has no trace methods by default"() {
        setup:
        def methods = new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET, false)
        when:
        def act = methods.getSupportedMethods().findAll { it.containsIgnoreCase("trace") }
        then:
        act.isEmpty()
    }

    def "Default eth methods are available"() {
        setup:
        def methods = new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET, false)
        expect:
        methods.isAvailable(method)
        where:
        method                                      | _
        "eth_gasPrice"                              | _
        "eth_estimateGas"                           | _
        "eth_getBlockTransactionCountByHash"        | _
        "eth_getUncleCountByBlockHash"              | _
        "eth_getBlockByHash"                        | _
        "eth_getBlockByNumber"                      | _
        "eth_getTransactionByBlockHashAndIndex"     | _
        "eth_getTransactionByBlockNumberAndIndex"   | _
        "eth_getStorageAt"                          | _
        "eth_getCode"                               | _
        "eth_getUncleByBlockHashAndIndex"           | _
        "eth_getLogs"                               | _
        "eth_maxPriorityFeePerGas"                  | _
        "eth_getTransactionByHash"                  | _
        "eth_getTransactionReceipt"                 | _
        "eth_call"                                  | _
        "eth_getTransactionCount"                   | _
        "eth_blockNumber"                           | _
        "eth_getBalance"                            | _
        "eth_sendRawTransaction"                    | _
        "eth_getBlockTransactionCountByNumber"      | _
        "eth_getUncleCountByBlockNumber"            | _
        "eth_getUncleByBlockNumberAndIndex"         | _
        "eth_feeHistory"                            | _
    }
}
