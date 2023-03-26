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
package io.emeraldpay.dshackle.monitoring.accesslog

import org.slf4j.LoggerFactory
import reactor.util.context.ContextView
import java.time.Instant
import java.util.Optional
import java.util.UUID
import java.util.function.Function
import io.grpc.Context as GrpcContext
import reactor.util.context.Context as ReactorContext

/**
 * Access to the current Request ID
 */
class AccessContext {

    companion object {
        private val log = LoggerFactory.getLogger(AccessContext::class.java)

        val REQUEST_ID_GRPC_KEY = GrpcContext.key<Value>("DSHACKLE REQ ID")
        private val REQUEST_ID_REACTOR_KEY = "DSHACKLE/MONITORING/ACCESS_REQ_ID"
    }

    data class Value(
        val id: UUID,
        val ts: Instant,
    )

    fun start(id: UUID): Function<ReactorContext, ReactorContext> {
        return setRequest(Value(id, Instant.now()))
    }

    fun setRequest(request: Value): Function<ReactorContext, ReactorContext> {
        return Function { ctx ->
            ctx.put(REQUEST_ID_REACTOR_KEY, request)
        }
    }

    /**
     * Copy ID from gRPC Context to Reactor Context
     */
    fun updateFromGrpc(): Function<ReactorContext, ReactorContext> {
        val requestId = REQUEST_ID_GRPC_KEY.get()
        return setRequest(requestId)
    }

    fun getRequest(ctx: ContextView): Value {
        return ctx.getOrEmpty<Value>(REQUEST_ID_REACTOR_KEY).or {
            Optional.of(Value(UUID.randomUUID(), Instant.now()))
        }.get()
    }
}
