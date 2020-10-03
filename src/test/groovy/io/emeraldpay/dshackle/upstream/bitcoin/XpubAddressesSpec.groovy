/**
 * Copyright (c) 2020 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.upstream.bitcoin

import org.bitcoinj.core.Address
import org.bitcoinj.params.MainNetParams
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Duration

class XpubAddressesSpec extends Specification {


    def "list all for mainnet"() {
        setup:
        XpubAddresses addresses = new XpubAddresses(Stub(AddressActiveCheck))

        when:
        // seed sustain pyramid victory drum primary silver safe wrestle section leg caught praise spring prepare input
        // account 0
        def act = addresses.allAddresses(
                "zpub6tsHyQGj1UmDcw9UavB31HSChH9rqMi8Qje5KPbnczzhFSZykmm3afivpVytxWubaabDih6AbGGhioiTCg5PssYXuVGiZ1vTVaMYvvQvvmz",
                0,
                4
        ).collectList().block()

        then:
        act.size() == 4
        act[0].toString() == "bc1qd6p79j20w2zy5zagptf5ksjkdmhx4d5sykrz0e"
        act[1].toString() == "bc1q54um5vzdjuwt6qvzvk2y5em72jedpktrvr24a3"
        act[2].toString() == "bc1q3yl03ugy6l5k3te5x340m70hyeqjzmmvksnuc8"
        act[3].toString() == "bc1qp0cgpkxl52r82ev9k2wpdzzk4m4nflwurj5yr8"
    }

    def "list all for testnet"() {
        setup:
        XpubAddresses addresses = new XpubAddresses(Stub(AddressActiveCheck))

        when:
        // seed sustain pyramid victory drum primary silver safe wrestle section leg caught praise spring prepare input
        // account 1
        def act = addresses.allAddresses(
                "vpub5aEsY5bNGvjkHPjdMiz8FbdoiT81kmF3JwuJ1LRdoFb7DxNh1qAHQCmMeDnr6RG2EcCwzGohem3oESxZGa6YWLPW79ryCyMrdYj54uUzNNq",
                0,
                4
        ).collectList().block()

        then:
        act.size() == 4
        act[0].toString() == "tb1q2phmcxl4tflgcrnm00jvrkh4a8n876vkjcv97m"
        act[1].toString() == "tb1qnhn0psv0uqtmg64tm258kqu0thurjvutcc80xs"
        act[2].toString() == "tb1qg62jwv77ul329pe4wcchr83zz02egwfh6syqsr"
        act[3].toString() == "tb1q8yg5qknrl5wmc98lw8dyfgrzamzaq8c9l0g44d"
    }

    def "list all for testnet - 2"() {
        setup:
        XpubAddresses addresses = new XpubAddresses(Stub(AddressActiveCheck))

        when:
        def act = addresses.allAddresses(
                "vpub5arxPHpfH2FKSNnBqyZJctzBtruGzM4sat7YKcQQNoNGgVZehD1tLiYGvhXBhPzKPcRDRjhGw94Dc9Wwob9BpbAMmkMX7Dzdfd5Ly9LHTGQ",
                0,
                12
        ).collectList().block()

        then:
        act.size() == 12
        act[0].toString() == "tb1qepcagv9wkp04ygq3ud33qrkk6482ulhkegc333"
        act[1].toString() == "tb1qtt6qxjk8dfafpr24skplms0fs4kr5m08vvef3l"
        act[2].toString() == "tb1qmsu5p4jtzhpl097pwafz384meeep7gmv35udya"
        act[3].toString() == "tb1qc784y7urnu74x250vy70204gdqw32t5kd3z97c"
        act[4].toString() == "tb1q380kh7jhdgx5248uapp64pp8nltx654hhvs4hp"
        act[5].toString() == "tb1qert744aljx4t0w2y0crz0q9xhcn64u0t3vveel"
        act[6].toString() == "tb1q6d7v77wlceknrmc3u86c0j46ynltqs8n4pvvrd"
        act[7].toString() == "tb1q62y8sxclnvzlyk89nt7spfzwsx8v9qqfdsavwv"
        act[8].toString() == "tb1q3q0wlg5mjtjwc0rcedj5zhu2wc4rch23fpffg8"
        act[9].toString() == "tb1qhhl5fty3utag39gspac903sxmmdq2tsxs63vfy"
        act[10].toString() == "tb1qlse5sm59a8hckv2y7np60ka8egsw05wkj4nhal"
        act[11].toString() == "tb1ql7y7syltstfn3t5svd56ywff4j65cg0mjylcjp"
    }

    def "list starting from some"() {
        setup:
        XpubAddresses addresses = new XpubAddresses(Stub(AddressActiveCheck))

        when:
        // seed sustain pyramid victory drum primary silver safe wrestle section leg caught praise spring prepare input
        // account 2
        def act = addresses.allAddresses(
                "zpub6tyKtxsQL16XpzxwFJSrVTxpKF4z8GL2WTqvdErC2iQBSEAzLqJNc8h6hg4EHMSPEavFDneR3JQjWjbZL7vtQrXMwN9bD4c9K9s1Z55aZfB",
                10,
                4
        ).collectList().block()

        then:
        act.size() == 4
        act[0].toString() == "bc1q3dzm92mnqvkyadwl3x5cqdvxaqa5h9c37qdlgs"
        act[1].toString() == "bc1q3kqug4cx95a02yhwn6geelftmw3zklrgmhjll8"
        act[2].toString() == "bc1q6znmcw8zzjv3asdylgkmt0k0xvhzl9q5awh0sq"
        act[3].toString() == "bc1q3fzlxvw803r8j44culrt7jz7q5y7zakushujmq"
    }

    def "list when only fist is active"() {
        setup:
        AddressActiveCheck check = Mock(AddressActiveCheck) {
            1 * isActive(Address.fromString(MainNetParams.get(), "bc1qaexx257l7sgm62szw2ulj6n2v99t5ph9ekkul3")) >> Mono.just(true)
            20 * isActive(_) >> Mono.just(false)
        }
        XpubAddresses addresses = new XpubAddresses(check)

        // seed sustain pyramid victory drum primary silver safe wrestle section leg caught praise spring prepare input
        // account 2
        when:
        def act = addresses.activeAddresses(
                "zpub6tyKtxsQL16XpzxwFJSrVTxpKF4z8GL2WTqvdErC2iQBSEAzLqJNc8h6hg4EHMSPEavFDneR3JQjWjbZL7vtQrXMwN9bD4c9K9s1Z55aZfB",
                0,
                50
        ).map { it.toString() }

        then:
        StepVerifier.create(act)
                .expectNext("bc1qaexx257l7sgm62szw2ulj6n2v99t5ph9ekkul3")
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "list when only 3rd is active"() {
        setup:
        AddressActiveCheck check = Mock(AddressActiveCheck) {
            1 * isActive(Address.fromString(MainNetParams.get(), "bc1qah84zz7aavf3f5eyx5f29809y6xugqyphq0wtz")) >> Mono.just(true)
            // 2 times before, 20 times after
            22 * isActive(_) >> Mono.just(false)
        }
        XpubAddresses addresses = new XpubAddresses(check)

        // seed sustain pyramid victory drum primary silver safe wrestle section leg caught praise spring prepare input
        // account 2
        when:
        def act = addresses.activeAddresses(
                "zpub6tyKtxsQL16XpzxwFJSrVTxpKF4z8GL2WTqvdErC2iQBSEAzLqJNc8h6hg4EHMSPEavFDneR3JQjWjbZL7vtQrXMwN9bD4c9K9s1Z55aZfB",
                0,
                50
        ).map { it.toString() }

        then:
        StepVerifier.create(act)
                .expectNext("bc1qah84zz7aavf3f5eyx5f29809y6xugqyphq0wtz")
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "list when only 22 is active"() {
        setup:
        AddressActiveCheck check = Mock(AddressActiveCheck) {
            // 2
            1 * isActive(Address.fromString(MainNetParams.get(), "bc1qah84zz7aavf3f5eyx5f29809y6xugqyphq0wtz")) >> Mono.just(true)
            // 11
            1 * isActive(Address.fromString(MainNetParams.get(), "bc1q3kqug4cx95a02yhwn6geelftmw3zklrgmhjll8")) >> Mono.just(true)
            // 22
            1 * isActive(Address.fromString(MainNetParams.get(), "bc1qf7m2rrmrksj34vhgxmm04y43dlj7c5f58f8ku6")) >> Mono.just(true)
            // 0..1 = 2
            // + 3..10 = 8
            // + 12..21 = 10
            // + 20
            40 * isActive(_) >> Mono.just(false)
        }
        XpubAddresses addresses = new XpubAddresses(check)

        // seed sustain pyramid victory drum primary silver safe wrestle section leg caught praise spring prepare input
        // account 2
        when:
        def act = addresses.activeAddresses(
                "zpub6tyKtxsQL16XpzxwFJSrVTxpKF4z8GL2WTqvdErC2iQBSEAzLqJNc8h6hg4EHMSPEavFDneR3JQjWjbZL7vtQrXMwN9bD4c9K9s1Z55aZfB",
                0,
                100
        ).map { it.toString() }

        then:
        StepVerifier.create(act)
                .expectNext("bc1qah84zz7aavf3f5eyx5f29809y6xugqyphq0wtz")
                .expectNext("bc1q3kqug4cx95a02yhwn6geelftmw3zklrgmhjll8")
                .expectNext("bc1qf7m2rrmrksj34vhgxmm04y43dlj7c5f58f8ku6")
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }
}
