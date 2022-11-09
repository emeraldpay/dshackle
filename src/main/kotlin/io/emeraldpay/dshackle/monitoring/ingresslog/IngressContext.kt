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
package io.emeraldpay.dshackle.monitoring.ingresslog

import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.config.IngressLogConfig
import io.emeraldpay.dshackle.monitoring.record.IngressRecord
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.grpc.Chain
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import reactor.util.context.Context
import reactor.util.context.ContextView
import java.time.Instant
import java.util.function.Function

/**
 * Monitoring for request made by Dshackle to upstreams
 */
class IngressContext {

    companion object {
        private val log = LoggerFactory.getLogger(IngressContext::class.java)
        private val INGRESS_CTX_KEY = "DSHACKLE/MONITORING/INGRESS"
        private val RPC_ID_KEY = "DSHACKLE/MONITORING/RPCID"
    }

    /**
     * A __MUTABLE__ container to keep actual JSON RPC ID that was used to access an upstream in the current context.
     */
    class RpcId {
        var rpcId: Int? = null
    }

    var includeParams: Boolean = IngressLogConfig.default().includeParams

    /**
     * Supposed to be set with an actual config on start, ex. from CurrentRequestLogWriter
     */
    var config: IngressLogConfig = IngressLogConfig.default()
        set(value) {
            field = value
            includeParams = value.includeParams
        }

    /**
     * Set an actual JSON RPC ID used to access an upstream
     */
    fun setRpcId(id: Int): Function<Context, Context> {
        return Function { ctx ->
            if (ctx.hasKey(RPC_ID_KEY)) {
                ctx.get<RpcId>(RPC_ID_KEY).rpcId = id
            }
            ctx
        }
    }

    /**
     * Populate the context with the initial data before calling an upstream
     */
    fun prepareForRpcCall(): Function<Context, Context> {
        return Function { ctx ->
            ctx.put(RPC_ID_KEY, RpcId())
        }
    }

    /**
     * Get the ID used to access the upstream as part of the current flow
     */
    fun getRpcId(ctx: ContextView): Int {
        if (ctx.hasKey(RPC_ID_KEY)) {
            return ctx.get<RpcId>(RPC_ID_KEY).rpcId ?: 0
        }
        return 0
    }

    fun getOrCreate(ctx: ContextView): IngressRecord.Builder {
        return if (!ctx.hasKey(INGRESS_CTX_KEY)) {
            IngressRecord.newBuilder()
                .copy(source = IngressRecord.Source.UNSET)
        } else {
            ctx.get(INGRESS_CTX_KEY)
        }
    }

    private fun update(modifier: (IngressRecord.Builder) -> IngressRecord.Builder): Function<Context, Context> {
        return Function { ctx ->
            val existing = getOrCreate(ctx)
            ctx.put(INGRESS_CTX_KEY, modifier(existing))
        }
    }

    fun cleanup(): Function<Context, Context> {
        return Function { ctx ->
            ctx.delete(INGRESS_CTX_KEY)
        }
    }

    fun isAvailable(ctx: ContextView): Boolean {
        return ctx.hasKey(INGRESS_CTX_KEY)
    }

    fun startCall(source: IngressRecord.Source): Function<Context, Context> {
        // prepare the value eagerly instead of the subscription moment, to make sure we have the timestamp of when the request was made
        val value = IngressRecord.newBuilder()
            .copy(source = source, ts = Instant.now())
        return Function { ctx ->
            val withRequest = Global.monitoring.egress.getRequest(ctx).let {
                value.copy(requestId = it.id)
            }
            ctx.put(INGRESS_CTX_KEY, withRequest)
        }
    }

    fun ensureInitialized(): Function<Context, Context> {
        return Function { ctx ->
            if (!ctx.hasKey(INGRESS_CTX_KEY)) {
                ctx.put(
                    INGRESS_CTX_KEY,
                    IngressRecord.newBuilder()
                        .copy(source = IngressRecord.Source.UNSET)
                )
            } else {
                ctx
            }
        }
    }

    fun startExecuting(): Function<Context, Context> {
        return update {
            it.copy(executeTs = Instant.now())
        }
    }

    fun withBlockchain(blockchain: Chain): Function<Context, Context> {
        return update {
            it.copy(blockchain = blockchain)
        }
    }

    fun withRequest(req: JsonRpcRequest): Function<Context, Context> {
        return Function { ctx ->
            withRequest(req.method, req.params).apply(ctx)
        }
    }

    fun withRequest(method: String, params: List<Any?>): Function<Context, Context> {
        return Function { ctx ->
            val paramsJson = if (includeParams) {
                try {
                    // cut extra long requests
                    StringUtils.abbreviateMiddle(
                        Global.objectMapper.writeValueAsString(params),
                        "..",
                        400
                    )
                } catch (t: Throwable) {
                    "<ERR: ${t.message}>"
                }
            } else {
                null
            }
            withRequest(method, paramsJson).apply(ctx)
        }
    }

    fun withRequest(method: String, paramsJson: String?): Function<Context, Context> {
        return update {
            it.requested(method, paramsJson)
        }
    }
}
