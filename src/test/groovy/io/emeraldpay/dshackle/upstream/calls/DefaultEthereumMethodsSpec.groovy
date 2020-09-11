package io.emeraldpay.dshackle.upstream.calls

import io.emeraldpay.grpc.Chain
import spock.lang.Specification

class DefaultEthereumMethodsSpec extends Specification {

    def "eth_chainId is hardcoded"() {
        setup:
        def methods = new DefaultEthereumMethods(Chain.ETHEREUM)
        when:
        def act = methods.isHardcoded("eth_chainId")
        then:
        act
    }

    def "Provides hardcoded correct chainId"() {
        expect:
        new String(new DefaultEthereumMethods(chain).executeHardcoded("eth_chainId")) == id
        where:
        chain                  | id
        Chain.ETHEREUM         | '"0x1"'
        Chain.ETHEREUM_CLASSIC | '"0x3d"'
        Chain.TESTNET_KOVAN    | '"0x2a"'
        Chain.TESTNET_GOERLI    | '"0x5"'
    }
}
