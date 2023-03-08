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

import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleRequest
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleResponse
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import org.bitcoinj.core.Address
import org.bitcoinj.params.MainNetParams
import reactor.core.publisher.Mono
import spock.lang.Specification

class RpcUnspentReaderSpec extends Specification {

    def "all if single address"() {
        setup:
        def json = this.class.getClassLoader().getResourceAsStream("bitcoin/unspent-one-addr.json").bytes
        def upstreams = Mock(BitcoinMultistream) {
            1 * read(new DshackleRequest(1, "listunspent", [1, 9999999, ["1K7xkspJg7DDKNwzXgoRSDCUxiFsRegsSK"]], null, RpcUnspentReader.selector)) >>
                    Mono.just(new DshackleResponse(1, json))
        }
        def reader = new RpcUnspentReader(upstreams)

        when:
        def act = reader.read(Address.fromString(new MainNetParams(), "1K7xkspJg7DDKNwzXgoRSDCUxiFsRegsSK")).block()

        then:
        // cat src/test/resources/bitcoin/unspent-one-addr.json | jq '. | length'
        act.size() == 36
        with(act[0]) {
            txid == "e0f946c8f971b25cdffa64eed71d886019e437c0bf6a1b280584c0be5d1b5409"
            vout == 29
            value == 1230030
        }
        with(act[1]) {
            txid == "66e1e4d14ed6f454d2fda036f35cba423274ecdf5d46deb93f172c412a0f650d"
            vout == 83
            value == 756339
        }
        with(act[35]) {
            txid == "f14b222e652c58d11435fa9172ddea000c6f5e20e6b715eb940fc28d1c4adeef"
            vout == 58
            value == 1105047
        }
    }

    def "select if two addresses - 35hK"() {
        setup:
        def json = this.class.getClassLoader().getResourceAsStream("bitcoin/unspent-two-addr.json").bytes
        def upstreams = Mock(BitcoinMultistream) {
            1 * read(new DshackleRequest(1, "listunspent", [1, 9999999, ["35hK24tcLEWcgNA4JxpvbkNkoAcDGqQPsP"]], null, RpcUnspentReader.selector)) >>
                    Mono.just(new DshackleResponse(1, json))
        }
        def reader = new RpcUnspentReader(upstreams)

        when:
        def act = reader.read(Address.fromString(new MainNetParams(), "35hK24tcLEWcgNA4JxpvbkNkoAcDGqQPsP")).block()

        then:
        // cat src/test/resources/bitcoin/unspent-two-addr.json | jq '[.[] | select(.address == "35hK24tcLEWcgNA4JxpvbkNkoAcDGqQPsP")] | length'
        act.size() == 340
        with(act[0]) {
            txid == "8ad0d954a01eeb4f2c62d58d291699af847f9c8df43b775c27ffe8a5f76eba00"
            vout == 1
            value == 216465
        }
        with(act[11]) {
            txid == "777671a46b30b068052a73387e036bc8515cd3ba6adf9be4c70dfc0699f67c09"
            vout == 0
            value == 307906
        }
        // cat src/test/resources/bitcoin/unspent-two-addr.json | jq '[.[] | select(.address == "35hK24tcLEWcgNA4JxpvbkNkoAcDGqQPsP")] | .[211]'
        with(act[211]) {
            txid == "f20727393b0a586a3062a615fb71f43ec21c24258c3c6ec546fee5cbc1fa2ba7"
            vout == 0
            value == 50000000000
        }
    }

    def "select if two addresses - 1K7x"() {
        setup:
        def json = this.class.getClassLoader().getResourceAsStream("bitcoin/unspent-two-addr.json").bytes
        def upstreams = Mock(BitcoinMultistream) {
            1 * read(_) >> Mono.just(new DshackleResponse(1, json))
        }
        def reader = new RpcUnspentReader(upstreams)

        when:
        def act = reader.read(Address.fromString(new MainNetParams(), "1K7xkspJg7DDKNwzXgoRSDCUxiFsRegsSK")).block()

        then:
        // cat src/test/resources/bitcoin/unspent-two-addr.json | jq '[.[] | select(.address == "1K7xkspJg7DDKNwzXgoRSDCUxiFsRegsSK")] | length'
        act.size() == 36
        // cat src/test/resources/bitcoin/unspent-two-addr.json | jq '[.[] | select(.address == "1K7xkspJg7DDKNwzXgoRSDCUxiFsRegsSK")] | .[0]'
        with(act[0]) {
            txid == "e0f946c8f971b25cdffa64eed71d886019e437c0bf6a1b280584c0be5d1b5409"
            vout == 29
            value == 1230030
        }
        // cat src/test/resources/bitcoin/unspent-two-addr.json | jq '[.[] | select(.address == "1K7xkspJg7DDKNwzXgoRSDCUxiFsRegsSK")] | .[35]'
        with(act[35]) {
            txid == "f14b222e652c58d11435fa9172ddea000c6f5e20e6b715eb940fc28d1c4adeef"
            vout == 58
            value == 1105047
        }
    }
}
