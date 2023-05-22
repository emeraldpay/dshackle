/**
 * Copyright (c) 2021 EmeraldPay, Inc
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

import io.emeraldpay.etherjar.domain.Wei
import io.emeraldpay.dshackle.upstream.ethereum.json.BlockJson
import io.emeraldpay.etherjar.rpc.json.TransactionJson
import spock.lang.Specification

class EthereumLegacyFeesSpec extends Specification {

    def "Extract fee from"() {
        setup:
        def block = new BlockJson()
        // 0x75cc01873a9818bf426a8b23d83450bf18530a822fd4fe9e86a416a5554176a6
        def tx = new TransactionJson().tap {
            it.gasPrice = Wei.ofUnits(8, Wei.Unit.GWEI)
        }

        def fees = new EthereumLegacyFees(Stub(EthereumMultistream), Stub(EthereumCachingReader), 10)
        when:
        def act = fees.extractFee(block, tx)
        then:
        act.priority == Wei.ofUnits(8, Wei.Unit.GWEI)
        act.max == Wei.ofUnits(8, Wei.Unit.GWEI)
        act.base == Wei.ZERO
    }
}
