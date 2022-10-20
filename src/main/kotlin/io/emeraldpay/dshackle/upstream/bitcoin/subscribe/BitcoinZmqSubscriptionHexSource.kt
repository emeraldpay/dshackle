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
package io.emeraldpay.dshackle.upstream.bitcoin.subscribe

import io.emeraldpay.dshackle.upstream.bitcoin.ZMQServer
import org.apache.commons.codec.binary.Hex
import reactor.core.publisher.Flux

class BitcoinZmqSubscriptionHexSource(
    topic: BitcoinZmqTopic,
    private val server: ZMQServer,
) : BitcoinSubscriptionConnect<String>(topic) {

    override fun createConnection(): Flux<String> {
        server.start()
        return server.getFlux()
            .map { Hex.encodeHexString(it) }
    }
}
