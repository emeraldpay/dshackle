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
package io.emeraldpay.dshackle.monitoring.record

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonPropertyOrder
import io.emeraldpay.dshackle.monitoring.Channel
import io.emeraldpay.dshackle.monitoring.requestlog.RequestType
import io.emeraldpay.grpc.Chain
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.UUID

class RequestRecord {

    companion object {
        private val log = LoggerFactory.getLogger(RequestRecord::class.java)

        fun newBuilder(): Builder {
            return Builder()
        }
    }

    @JsonPropertyOrder("version", "id", "success", "upstream", "request", "jsonrpc")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    data class BlockchainRequest(
        val id: UUID,
        val request: RequestDetails,
        val blockchain: Chain? = null,
        val jsonrpc: JsonRpcDetails?,

        // null for subscription responses
        val execute: Instant?,
        val complete: Instant,

        val upstream: UpstreamDetails?,

        val success: Boolean,
        val responseSize: Int = 0,
        val error: ErrorDetails? = null,
    ) {
        val version = "requestlog/v1alpha"

        val queueTime: Double = if (execute != null) {
            (execute.nano - request.start.nano).coerceAtLeast(0) / 1_000_000.0
        } else {
            (complete.nano - request.start.nano).coerceAtLeast(0) / 1_000_000.0
        }
        val requestTime: Double? = if (execute != null) {
            (complete.nano - execute.nano).coerceAtLeast(0) / 1_000_000.0
        } else {
            null
        }
    }

    @JsonPropertyOrder("source", "id", "start")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    data class RequestDetails(
        val id: UUID,
        val start: Instant,
        val source: Source
    )

    data class UpstreamDetails(
        val id: String,
        val channel: Channel,
        val type: RequestType,
    )

    data class Builder(
        val ts: Instant = Instant.now(),
        val source: Source = Source.UNSET,

        // Reference to the source request. In most cases it's the id of the request to the Dshackle, but for internal
        // requests such as a health check, it's a new random id.
        val requestId: UUID? = null,
        val rpc: JsonRpcDetails = JsonRpcDetails(),
        val channel: Channel? = null,
        val upstreamId: String = "",
        val type: RequestType? = null,
        val executeTs: Instant? = null,
        val completeTs: Instant? = null,
        val error: ErrorDetails? = null,
        val blockchain: Chain = Chain.UNSPECIFIED,
        val responseSize: Int = 0,
    ) {

        fun requested(method: String, params: String?): Builder =
            this.copy(rpc = JsonRpcDetails(method, params))

        fun build(): BlockchainRequest {
            requireNotNull(channel)
            requireNotNull(type)
            requireNotNull(requestId)

            return BlockchainRequest(
                id = UUID.randomUUID(),
                request = RequestDetails(requestId, ts, source),
                upstream = UpstreamDetails(upstreamId, channel, type),
                execute = executeTs,
                complete = completeTs ?: Instant.now(),
                jsonrpc = rpc,
                success = error == null,
                error = error,
                responseSize = responseSize,
                blockchain = blockchain
            )
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    data class JsonRpcDetails(
        val method: String? = null,
        // serialized as JSON
        val params: String? = null,
        val id: Int? = null,
    ) {
        fun updateFrom(another: JsonRpcDetails): JsonRpcDetails {
            return JsonRpcDetails(
                another.method ?: this.method,
                another.params ?: this.params,
                another.id ?: this.id
            )
        }
    }

    data class ErrorDetails(
        val code: Int,
        val message: String? = null
    )

    enum class Source {
        /**
         * For requests made to the Dshackle
         */
        REQUEST,

        /**
         * Internal request from the Dshackle itself, such as health checks
         */
        INTERNAL,

        /**
         * A default placeholder. If it comes to the log it means there is a missing part that sets the actual source
         */
        UNSET,
    }
}
