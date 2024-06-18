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
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.ChainResponse
import io.emeraldpay.dshackle.upstream.beaconchain.BeaconChainBlockHeader
import io.emeraldpay.dshackle.upstream.beaconchain.BeaconChainBlockHeaderDeserializer
import io.emeraldpay.dshackle.upstream.bitcoin.data.EsploraUnspent
import io.emeraldpay.dshackle.upstream.bitcoin.data.EsploraUnspentDeserializer
import io.emeraldpay.dshackle.upstream.bitcoin.data.RpcUnspent
import io.emeraldpay.dshackle.upstream.bitcoin.data.RpcUnspentDeserializer
import io.emeraldpay.dshackle.upstream.ethereum.domain.TransactionId
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.json.TransactionIdSerializer
import java.text.SimpleDateFormat
import java.util.Locale
import java.util.TimeZone
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService

class Global {

    companion object {

        val nullValue: ByteArray = "null".toByteArray()

        var metricsExtended = false

        fun chainById(id: String?): Chain {
            if (id == null) {
                return Chain.UNSPECIFIED
            }
            return Chain.values().find { chain ->
                chain.shortNames.contains(id.lowercase(Locale.getDefault()).replace("_", "-").trim())
            } ?: Chain.UNSPECIFIED
        }

        fun chainByChainId(id: String): Chain {
            return Chain.values().find { chain ->
                chain.chainId.lowercase() == id.lowercase()
            } ?: Chain.UNSPECIFIED
        }

        @JvmStatic
        val objectMapper: ObjectMapper = createObjectMapper()

        var version: String = "DEV"

        val control: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()

        private fun createObjectMapper(): ObjectMapper {
            val module = SimpleModule("EmeraldDshackle", Version(1, 0, 0, null, null, null))
            module.addSerializer(ChainResponse::class.java, ChainResponse.ResponseJsonSerializer())
            module.addSerializer(TransactionId::class.java, TransactionIdSerializer())

            module.addDeserializer(EsploraUnspent::class.java, EsploraUnspentDeserializer())
            module.addDeserializer(RpcUnspent::class.java, RpcUnspentDeserializer())
            module.addDeserializer(ChainRequest::class.java, ChainRequest.Deserializer())
            module.addDeserializer(BeaconChainBlockHeader::class.java, BeaconChainBlockHeaderDeserializer())

            val objectMapper = ObjectMapper()
            objectMapper.registerModule(module)
            objectMapper.registerModule(Jdk8Module())
            objectMapper.registerModule(JavaTimeModule())
            objectMapper.registerKotlinModule()
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            objectMapper
                .setDateFormat(SimpleDateFormat("yyyy-MM-dd\'T\'HH:mm:ss.SSS"))
                .setTimeZone(TimeZone.getTimeZone("UTC"))

            return objectMapper
        }
    }
}
