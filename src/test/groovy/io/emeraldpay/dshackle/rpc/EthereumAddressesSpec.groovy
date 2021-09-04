package io.emeraldpay.dshackle.rpc

import io.emeraldpay.api.proto.Common
import io.emeraldpay.etherjar.domain.Address
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Duration

class EthereumAddressesSpec extends Specification {

    EthereumAddresses ethereumAddresses = new EthereumAddresses()

    def "Extract single address"() {
        setup:
        def address = Common.AnyAddress.newBuilder()
                .setAddressSingle(
                        Common.SingleAddress.newBuilder()
                                .setAddress("0xab5c66752a9e8167967685f1450532fb96d5d24f")
                )
                .build()
        when:
        def act = ethereumAddresses.extract(address)
        then:
        StepVerifier.create(act)
                .expectNext(Address.from("0xab5c66752a9e8167967685f1450532fb96d5d24f"))
                .expectComplete().verify(Duration.ofSeconds(1))

    }

    def "Extract single addresses passed as multi"() {
        setup:
        def address = Common.AnyAddress.newBuilder()
                .setAddressMulti(
                        Common.MultiAddress.newBuilder()
                                .addAddresses(
                                        Common.SingleAddress.newBuilder()
                                                .setAddress("0xab5c66752a9e8167967685f1450532fb96d5d24f")
                                )
                )
                .build()
        when:
        def act = ethereumAddresses.extract(address)
        then:
        StepVerifier.create(act)
                .expectNext(Address.from("0xab5c66752a9e8167967685f1450532fb96d5d24f"))
                .expectComplete().verify(Duration.ofSeconds(1))

    }

    def "Extract two addresses"() {
        setup:
        def address = Common.AnyAddress.newBuilder()
                .setAddressMulti(
                        Common.MultiAddress.newBuilder()
                                .addAddresses(
                                        Common.SingleAddress.newBuilder()
                                                .setAddress("0xab5c66752a9e8167967685f1450532fb96d5d24f")
                                )
                                .addAddresses(
                                        Common.SingleAddress.newBuilder()
                                                .setAddress("0xfdb16996831753d5331ff813c29a93c76834a0ad")
                                )
                )
                .build()
        when:
        def act = ethereumAddresses.extract(address)
        then:
        StepVerifier.create(act)
                .expectNext(Address.from("0xab5c66752a9e8167967685f1450532fb96d5d24f"))
                .expectNext(Address.from("0xfdb16996831753d5331ff813c29a93c76834a0ad"))
                .expectComplete().verify(Duration.ofSeconds(1))
    }

    def "Extract fes addresses"() {
        setup:
        def address = Common.AnyAddress.newBuilder()
                .setAddressMulti(
                        Common.MultiAddress.newBuilder()
                                .addAddresses(
                                        Common.SingleAddress.newBuilder()
                                                .setAddress("0xab5c66752a9e8167967685f1450532fb96d5d24f")
                                )
                                .addAddresses(
                                        Common.SingleAddress.newBuilder()
                                                .setAddress("0xfdb16996831753d5331ff813c29a93c76834a0ad")
                                )
                                .addAddresses(
                                        Common.SingleAddress.newBuilder()
                                                .setAddress("0x46705dfff24256421a05d056c29e81bdc09723b8")
                                )
                                .addAddresses(
                                        Common.SingleAddress.newBuilder()
                                                .setAddress("0xadb2b42f6bd96f5c65920b9ac88619dce4166f94")
                                )
                                .addAddresses(
                                        Common.SingleAddress.newBuilder()
                                                .setAddress("0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852")
                                )
                )
                .build()
        when:
        def act = ethereumAddresses.extract(address)
        then:
        StepVerifier.create(act)
                .expectNext(Address.from("0xab5c66752a9e8167967685f1450532fb96d5d24f"))
                .expectNext(Address.from("0xfdb16996831753d5331ff813c29a93c76834a0ad"))
                .expectNext(Address.from("0x46705dfff24256421a05d056c29e81bdc09723b8"))
                .expectNext(Address.from("0xadb2b42f6bd96f5c65920b9ac88619dce4166f94"))
                .expectNext(Address.from("0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852"))
                .expectComplete().verify(Duration.ofSeconds(1))
    }
}
