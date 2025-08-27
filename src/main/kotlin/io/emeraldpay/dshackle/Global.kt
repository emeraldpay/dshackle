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
package io.emeraldpay.dshackle

import com.fasterxml.jackson.core.Version
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import io.emeraldpay.api.Chain
import io.emeraldpay.dshackle.monitoring.MonitoringContext
import io.emeraldpay.dshackle.upstream.bitcoin.data.EsploraUnspent
import io.emeraldpay.dshackle.upstream.bitcoin.data.EsploraUnspentDeserializer
import io.emeraldpay.dshackle.upstream.bitcoin.data.RpcUnspent
import io.emeraldpay.dshackle.upstream.bitcoin.data.RpcUnspentDeserializer
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.etherjar.rpc.EtherjarModule
import java.text.SimpleDateFormat
import java.util.Locale
import java.util.TimeZone
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService

class Global {
    companion object {
        var metricsExtended = false

        val chainNames =
            mapOf(
                "ethereum" to Chain.ETHEREUM,
                "ethereum-classic" to Chain.ETHEREUM_CLASSIC,
                "eth" to Chain.ETHEREUM,
                "polygon" to Chain.MATIC,
                "matic" to Chain.MATIC,
                "etc" to Chain.ETHEREUM_CLASSIC,
                "morden" to Chain.TESTNET_MORDEN,
                "kovan" to Chain.TESTNET_KOVAN,
                "kovan-testnet" to Chain.TESTNET_KOVAN,
                "goerli" to Chain.TESTNET_GOERLI,
                "goerli-testnet" to Chain.TESTNET_GOERLI,
                "holesky" to Chain.TESTNET_HOLESKY,
                "holesky-testnet" to Chain.TESTNET_HOLESKY,
                "sepolia" to Chain.TESTNET_SEPOLIA,
                "sepolia-testnet" to Chain.TESTNET_SEPOLIA,
                "hoodi" to Chain.TESTNET_HOODI,
                "hoodi-testnet" to Chain.TESTNET_HOODI,
                "rinkeby" to Chain.TESTNET_RINKEBY,
                "rinkeby-testnet" to Chain.TESTNET_RINKEBY,
                "ropsten" to Chain.TESTNET_ROPSTEN,
                "ropsten-testnet" to Chain.TESTNET_ROPSTEN,
                "bitcoin" to Chain.BITCOIN,
                "bitcoin-testnet" to Chain.TESTNET_BITCOIN,
                "bitcoin-testnet-3" to Chain.TESTNET_BITCOIN,
                "bitcoin-testnet-v3" to Chain.TESTNET_BITCOIN,
                "bitcoin-testnet-4" to Chain.TESTNET_BITCOIN_V4,
                "bitcoin-testnet-v4" to Chain.TESTNET_BITCOIN_V4,
            )

        fun chainById(id: String?): Chain {
            if (id == null) {
                return Chain.UNSPECIFIED
            }
            return chainNames[
                id.lowercase(Locale.getDefault()).replace("_", "-").trim(),
            ] ?: Chain.UNSPECIFIED
        }

        @JvmStatic
        val objectMapper: ObjectMapper = createObjectMapper()

        var version: String = "DEV"

        val control: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()

        val monitoring: MonitoringContext = MonitoringContext()

        private fun createObjectMapper(): ObjectMapper {
            val module = SimpleModule("EmeraldDshackle", Version(1, 0, 0, null, null, null))
            module.addSerializer(JsonRpcResponse::class.java, JsonRpcResponse.ResponseJsonSerializer())

            module.addDeserializer(EsploraUnspent::class.java, EsploraUnspentDeserializer())
            module.addDeserializer(RpcUnspent::class.java, RpcUnspentDeserializer())
            module.addDeserializer(JsonRpcRequest::class.java, JsonRpcRequest.Deserializer())

            val objectMapper = ObjectMapper()
            objectMapper.registerModule(module)
            objectMapper.registerModule(EtherjarModule())
            objectMapper.registerModule(Jdk8Module())
            objectMapper.registerModule(JavaTimeModule())
            objectMapper.registerModule(KotlinModule.Builder().build())
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            objectMapper
                .setDateFormat(SimpleDateFormat("yyyy-MM-dd\'T\'HH:mm:ss.SSS"))
                .setTimeZone(TimeZone.getTimeZone("UTC"))

            return objectMapper
        }
    }
}
