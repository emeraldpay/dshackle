package io.emeraldpay.dshackle.upstream.calls

import io.emeraldpay.grpc.Chain
import spock.lang.Specification

class DefaultEthereumMethodsSpec extends Specification {

    def "eth_chainId is available"() {
        setup:
        def methods = new DefaultEthereumMethods(Chain.ETHEREUM)
        when:
        def act = methods.isAvailable("eth_chainId")
        then:
        act
    }

    def "eth_chainId is hardcoded"() {
        setup:
        def methods = new DefaultEthereumMethods(Chain.ETHEREUM)
        when:
        def act = methods.isHardcoded("eth_chainId")
        then:
        act
    }

    def "eth_chainId is not callable"() {
        setup:
        def methods = new DefaultEthereumMethods(Chain.ETHEREUM)
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
        Chain.ETHEREUM         | '"0x1"'
        Chain.ETHEREUM_CLASSIC | '"0x3d"'
        Chain.TESTNET_KOVAN    | '"0x2a"'
        Chain.BSC              | '"0x38"'
        Chain.TESTNET_GOERLI   | '"0x5"'
        Chain.TESTNET_RINKEBY  | '"0x4"'
        Chain.TESTNET_ROPSTEN  | '"0x3"'
    }
}
