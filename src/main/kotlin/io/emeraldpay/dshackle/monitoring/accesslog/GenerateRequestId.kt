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

import io.grpc.Context
import io.grpc.Contexts
import io.grpc.Metadata
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.ServerInterceptor
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.UUID

/**
 * Generates a new random ID for the request and populates it into gRPC Context. Later in can be read from the Context and associated with other details.
 *
 * @see AccessContext to access the ID
 * @see io.emeraldpay.dshackle.rpc.BlockchainRpc which puts it using EgressContext into the Reactor context
 */
class GenerateRequestId : ServerInterceptor {
    companion object {
        private val log = LoggerFactory.getLogger(GenerateRequestId::class.java)
    }

    override fun <ReqT : Any?, RespT : Any?> interceptCall(
        call: ServerCall<ReqT, RespT>,
        headers: Metadata,
        next: ServerCallHandler<ReqT, RespT>,
    ): ServerCall.Listener<ReqT> {
        val ctx =
            Context
                .current()
                .withValue(AccessContext.REQUEST_ID_GRPC_KEY, AccessContext.Value(UUID.randomUUID(), Instant.now()))

        return Contexts.interceptCall(ctx, call, headers, next)
    }
}
