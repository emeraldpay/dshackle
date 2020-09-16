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
import org.bitcoinj.core.ECKey
import org.bitcoinj.core.NetworkParameters
import org.bitcoinj.crypto.ChildNumber
import org.bitcoinj.crypto.DeterministicKey
import org.bitcoinj.crypto.HDKeyDerivation
import org.bitcoinj.params.MainNetParams
import org.bitcoinj.params.TestNet3Params
import org.bitcoinj.script.Script
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.util.function.Tuples
import java.util.concurrent.atomic.AtomicInteger

open class XpubAddresses(
        private val addressActiveCheck: AddressActiveCheck
) {

    companion object {
        private val log = LoggerFactory.getLogger(XpubAddresses::class.java)
        private val MAINNET = MainNetParams()
        private val TESTNET = TestNet3Params()
        private val INACTIVE_LIMIT = 20
    }

    open fun allAddresses(xpub: String, start: Int, limit: Int): Flux<Address> {
        // versions:
        // https://electrum.readthedocs.io/en/latest/xpub_version_bytes.html
        // TODO doesn't support SH keys right now. should?
        val prefix = xpub.substring(0, 4)
        val type: Script.ScriptType
        val network: NetworkParameters

        when (prefix) {
            "xpub" -> {
                type = Script.ScriptType.P2PKH
                network = MAINNET
            }
            "zpub" -> {
                type = Script.ScriptType.P2WPKH
                network = MAINNET
            }
            "tpub" -> {
                type = Script.ScriptType.P2PKH
                network = TESTNET
            }
            "vpub" -> {
                type = Script.ScriptType.P2WPKH
                network = TESTNET
            }
            else -> return Flux.error(IllegalArgumentException("Unsupported type: $prefix"))
        }

        val key: DeterministicKey
        try {
            key = DeterministicKey.deserializeB58(xpub, network)
        } catch (t: Throwable) {
            return Flux.error(t)
        }

        return Flux.range(start, limit)
                .map { HDKeyDerivation.deriveChildKey(key, ChildNumber(it, false)) }
                .map { Address.fromKey(network, ECKey.fromPublicOnly(it.pubKeyPoint), type) }
    }

    open fun activeAddresses(xpub: String, start: Int, limit: Int): Flux<Address> {
        val lastActive = AtomicInteger(0)
        return this.allAddresses(xpub, start, limit)
                .zipWith(Flux.range(0, limit))
                .takeUntil {
                    it.t2 - lastActive.get() >= INACTIVE_LIMIT
                }
                .concatMap { toCheck ->
                    addressActiveCheck.isActive(toCheck.t1)
                            .doOnNext { active -> if (active) lastActive.set(toCheck.t2) }
                            .map { Tuples.of(toCheck.t1, it) }
                }
                .filter {
                    it.t2
                }
                .map {
                    it.t1
                }
    }

}