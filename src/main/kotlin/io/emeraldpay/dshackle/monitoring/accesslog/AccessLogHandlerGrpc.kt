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
package io.emeraldpay.dshackle.monitoring.accesslog

import io.emeraldpay.dshackle.monitoring.Channel
import io.grpc.ForwardingServerCall
import io.grpc.ForwardingServerCallListener
import io.grpc.Metadata
import io.grpc.MethodDescriptor
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.ServerInterceptor
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import java.util.UUID

@Service
class AccessLogHandlerGrpc(
    @Autowired private val accessLogWriter: CurrentAccessLogWriter,
) : ServerInterceptor {
    companion object {
        private val log = LoggerFactory.getLogger(AccessLogHandlerGrpc::class.java)
    }

    override fun <ReqT : Any, RespT : Any> interceptCall(
        call: ServerCall<ReqT, RespT>,
        headers: Metadata,
        next: ServerCallHandler<ReqT, RespT>,
    ): ServerCall.Listener<ReqT> {
        val requestId = AccessContext.REQUEST_ID_GRPC_KEY.get().id

        return when (val method = call.methodDescriptor.bareMethodName) {
            "SubscribeHead" -> processSubscribeHead(call, headers, next, requestId)
            "SubscribeBalance" -> processSubscribeBalance(call, headers, next, true, requestId)
            "SubscribeTxStatus" -> processSubscribeTxStatus(call, headers, next, requestId)
            "GetBalance" -> processSubscribeBalance(call, headers, next, false, requestId)
            "NativeCall" -> processNativeCall(call, headers, next, requestId)
            "NativeSubscribe" -> processNativeSubscribe(call, headers, next, requestId)
            "Describe" -> processDescribe(call, headers, next, requestId)
            "SubscribeStatus" -> processStatus(call, headers, next, requestId)
            "EstimateFee" -> processEstimateFee(call, headers, next, requestId)
            "SubscribeAddressAllowance" -> processSubscribeAddressAllowance(call, headers, next, true, requestId)
            "GetAddressAllowance" -> processSubscribeAddressAllowance(call, headers, next, false, requestId)
            else -> {
                log.warn("unsupported method `{}`", method)
                next.startCall(call, headers)
            }
        }
    }

    private fun <ReqT : Any, RespT : Any, E> process(
        call: ServerCall<ReqT, RespT>,
        headers: Metadata,
        next: ServerCallHandler<ReqT, RespT>,
        builder: RecordBuilder.RequestReply<E, ReqT, RespT>,
    ): ServerCall.Listener<ReqT> {
        builder.start(headers, call.attributes)
        val callWrapper: ServerCall<ReqT, RespT> =
            StdCallResponse(
                call,
                builder,
                accessLogWriter,
            )
        return StdCallListener(
            next.startCall(callWrapper, headers),
            builder,
        )
    }

    @Suppress("UNCHECKED_CAST")
    private fun <ReqT : Any, RespT : Any> processSubscribeHead(
        call: ServerCall<ReqT, RespT>,
        headers: Metadata,
        next: ServerCallHandler<ReqT, RespT>,
        requestId: UUID,
    ): ServerCall.Listener<ReqT> =
        process(
            call,
            headers,
            next,
            RecordBuilder.SubscribeHead(requestId) as RecordBuilder.RequestReply<*, ReqT, RespT>,
        )

    @Suppress("UNCHECKED_CAST")
    private fun <ReqT : Any, RespT : Any> processSubscribeBalance(
        call: ServerCall<ReqT, RespT>,
        headers: Metadata,
        next: ServerCallHandler<ReqT, RespT>,
        subscribe: Boolean,
        requestId: UUID,
    ): ServerCall.Listener<ReqT> =
        process(
            call,
            headers,
            next,
            RecordBuilder.SubscribeBalance(subscribe, requestId) as RecordBuilder.RequestReply<*, ReqT, RespT>,
        )

    @Suppress("UNCHECKED_CAST")
    private fun <ReqT : Any, RespT : Any> processSubscribeAddressAllowance(
        call: ServerCall<ReqT, RespT>,
        headers: Metadata,
        next: ServerCallHandler<ReqT, RespT>,
        subscribe: Boolean,
        requestId: UUID,
    ): ServerCall.Listener<ReqT> =
        process(
            call,
            headers,
            next,
            RecordBuilder.SubscribeAddressAllowance(subscribe, requestId) as RecordBuilder.RequestReply<*, ReqT, RespT>,
        )

    @Suppress("UNCHECKED_CAST")
    private fun <ReqT : Any, RespT : Any> processSubscribeTxStatus(
        call: ServerCall<ReqT, RespT>,
        headers: Metadata,
        next: ServerCallHandler<ReqT, RespT>,
        requestId: UUID,
    ): ServerCall.Listener<ReqT> =
        process(
            call,
            headers,
            next,
            RecordBuilder.TxStatus(requestId) as RecordBuilder.RequestReply<*, ReqT, RespT>,
        )

    @Suppress("UNCHECKED_CAST")
    private fun <ReqT : Any, RespT : Any> processNativeCall(
        call: ServerCall<ReqT, RespT>,
        headers: Metadata,
        next: ServerCallHandler<ReqT, RespT>,
        requestId: UUID,
    ): ServerCall.Listener<ReqT> =
        process(
            call,
            headers,
            next,
            RecordBuilder.NativeCall(requestId) as RecordBuilder.RequestReply<*, ReqT, RespT>,
        )

    @Suppress("UNCHECKED_CAST")
    private fun <ReqT : Any, RespT : Any> processNativeSubscribe(
        call: ServerCall<ReqT, RespT>,
        headers: Metadata,
        next: ServerCallHandler<ReqT, RespT>,
        requestId: UUID,
    ): ServerCall.Listener<ReqT> =
        process(
            call,
            headers,
            next,
            RecordBuilder.NativeSubscribe(Channel.DSHACKLE, requestId) as RecordBuilder.RequestReply<*, ReqT, RespT>,
        )

    @Suppress("UNCHECKED_CAST")
    private fun <ReqT : Any, RespT : Any> processDescribe(
        call: ServerCall<ReqT, RespT>,
        headers: Metadata,
        next: ServerCallHandler<ReqT, RespT>,
        requestId: UUID,
    ): ServerCall.Listener<ReqT> =
        process(
            call,
            headers,
            next,
            RecordBuilder.Describe(requestId) as RecordBuilder.RequestReply<*, ReqT, RespT>,
        )

    @Suppress("UNCHECKED_CAST")
    private fun <ReqT : Any, RespT : Any> processStatus(
        call: ServerCall<ReqT, RespT>,
        headers: Metadata,
        next: ServerCallHandler<ReqT, RespT>,
        requestId: UUID,
    ): ServerCall.Listener<ReqT> =
        process(
            call,
            headers,
            next,
            RecordBuilder.Status(requestId) as RecordBuilder.RequestReply<*, ReqT, RespT>,
        )

    @Suppress("UNCHECKED_CAST")
    private fun <ReqT : Any, RespT : Any> processEstimateFee(
        call: ServerCall<ReqT, RespT>,
        headers: Metadata,
        next: ServerCallHandler<ReqT, RespT>,
        requestId: UUID,
    ): ServerCall.Listener<ReqT> =
        process(
            call,
            headers,
            next,
            RecordBuilder.EstimateFee(requestId) as RecordBuilder.RequestReply<*, ReqT, RespT>,
        )

    open class StdCallListener<Req, EB : RecordBuilder.RequestReply<*, Req, *>>(
        val next: ServerCall.Listener<Req>,
        val builder: EB,
    ) : ForwardingServerCallListener<Req>() {
        override fun onMessage(message: Req) {
            builder.onRequest(message)
            super.onMessage(message)
        }

        override fun delegate(): ServerCall.Listener<Req> = next
    }

    open class StdCallResponse<ReqT : Any, RespT : Any, EB : RecordBuilder.RequestReply<*, ReqT, RespT>>(
        val next: ServerCall<ReqT, RespT>,
        val builder: EB,
        val accessLogWriter: CurrentAccessLogWriter,
    ) : ForwardingServerCall<ReqT, RespT>() {
        override fun getMethodDescriptor(): MethodDescriptor<ReqT, RespT> = next.methodDescriptor

        override fun delegate(): ServerCall<ReqT, RespT> = next

        override fun sendMessage(message: RespT) {
            super.sendMessage(message)
            accessLogWriter.submit(
                builder.onReply(message)!!,
            )
        }
    }
}
