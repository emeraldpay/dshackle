package io.emeraldpay.dshackle

import io.emeraldpay.grpc.Chain
import spock.lang.Specification

class BlockchainTypeSpec extends Specification {

    def "Correct type for ethereum"() {
        expect:
        BlockchainType.fromBlockchain(chain) == BlockchainType.ETHEREUM
        where:
        chain << [Chain.ETHEREUM, Chain.ETHEREUM_CLASSIC, Chain.TESTNET_KOVAN, Chain.TESTNET_MORDEN]
    }

    def "Correct type for bitcoin"() {
        expect:
        BlockchainType.fromBlockchain(chain) == BlockchainType.BITCOIN
        where:
        chain << [Chain.BITCOIN, Chain.TESTNET_BITCOIN]
    }

}
