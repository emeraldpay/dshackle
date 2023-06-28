package io.emeraldpay.dshackle.upstream.calls

import io.emeraldpay.dshackle.Chain
import spock.lang.Specification

class DefaultEthereumMethodsSpec extends Specification {

    def "eth_chainId is available"() {
        setup:
        def methods = new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET)
        when:
        def act = methods.isAvailable("eth_chainId")
        then:
        act
    }

    def "eth_chainId is hardcoded"() {
        setup:
        def methods = new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET)
        when:
        def act = methods.isHardcoded("eth_chainId")
        then:
        act
    }

    def "eth_chainId is not callable"() {
        setup:
        def methods = new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET)
        when:
        def act = methods.isCallable("eth_chainId")
        then:
        !act
    }

    def "Provides hardcoded correct chainId"() {
        expect:
        new String(new DefaultEthereumMethods(chain).executeHardcoded("eth_chainId")) == id
        where:
        chain                  | id
        Chain.ETHEREUM__MAINNET | '"0x1"'
        Chain.ETHEREUM_CLASSIC__MAINNET | '"0x3d"'
        Chain.ETHEREUM__KOVAN | '"0x2a"'
        Chain.ETHEREUM__GOERLI | '"0x5"'
        Chain.ETHEREUM__RINKEBY | '"0x4"'
        Chain.ETHEREUM__ROPSTEN | '"0x3"'
    }

    def "Optimism chain unsupported methods"() {
        setup:
        def methods = new DefaultEthereumMethods(Chain.OPTIMISM__MAINNET)
        when:
        def acc = methods.isAvailable("eth_getAccounts")
        def trans = methods.isAvailable("eth_sendTransaction")
        then:
        !acc
        !trans
    }

    def "Has supported specific methods"() {
        expect:
        new DefaultEthereumMethods(chain).getSupportedMethods().containsAll(methods)
        where:
        chain          | methods
        Chain.POLYGON_POS__MAINNET | ["bor_getAuthor",
                                      "bor_getCurrentValidators",
                                      "bor_getCurrentProposer",
                                      "bor_getRootHash",
                                      "bor_getSignersAtHash",
                                      "eth_getRootHash"]
        Chain.OPTIMISM__MAINNET | ["rollup_gasPrices"]
    }

    def "Has no filter methods by default"() {
        setup:
        def methods = new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET)
        when:
        def act = methods.getSupportedMethods().findAll { it.containsIgnoreCase("filter") }
        then:
        act.isEmpty()
    }

    def "Has no trace methods by default"() {
        setup:
        def methods = new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET)
        when:
        def act = methods.getSupportedMethods().findAll { it.containsIgnoreCase("trace") }
        then:
        act.isEmpty()
    }
}
