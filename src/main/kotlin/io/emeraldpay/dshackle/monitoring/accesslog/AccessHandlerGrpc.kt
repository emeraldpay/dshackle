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
import java.time.Instant

@Service
class AccessHandlerGrpc(
    @Autowired private val accessLogWriter: AccessLogWriter
) : ServerInterceptor {

    companion object {
        private val log = LoggerFactory.getLogger(AccessHandlerGrpc::class.java)
    }

    override fun <ReqT : Any, RespT : Any> interceptCall(
        call: ServerCall<ReqT, RespT>,
        headers: Metadata,
        next: ServerCallHandler<ReqT, RespT>
    ): ServerCall.Listener<ReqT> {

        return when (val method = call.methodDescriptor.bareMethodName) {
            "SubscribeHead" -> processSubscribeHead(call, headers, next)
            "SubscribeBalance" -> processSubscribeBalance(call, headers, next, true)
            "SubscribeTxStatus" -> processSubscribeTxStatus(call, headers, next)
            "GetBalance" -> processSubscribeBalance(call, headers, next, false)
            "NativeCall" -> processNativeCall(call, headers, next)
            "NativeSubscribe" -> processNativeSubscribe(call, headers, next)
            "Describe" -> processDescribe(call, headers, next)
            "SubscribeStatus" -> processStatus(call, headers, next)
            "EstimateFee" -> processEstimateFee(call, headers, next)
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
        builder: EventsBuilder.RequestReply<E, ReqT, RespT>
    ): ServerCall.Listener<ReqT> {
        builder.start(headers, call.attributes)
        val callWrapper: ServerCall<ReqT, RespT> = StdCallResponse(
            call, builder, accessLogWriter
        )
        return StdCallListener(
            next.startCall(callWrapper, headers),
            builder
        )
    }

    @Suppress("UNCHECKED_CAST")
    private fun <ReqT : Any, RespT : Any> processSubscribeHead(
        call: ServerCall<ReqT, RespT>,
        headers: Metadata,
        next: ServerCallHandler<ReqT, RespT>
    ): ServerCall.Listener<ReqT> {
        return process(
            call, headers, next,
            EventsBuilder.SubscribeHead() as EventsBuilder.RequestReply<*, ReqT, RespT>
        )
    }

    @Suppress("UNCHECKED_CAST")
    private fun <ReqT : Any, RespT : Any> processSubscribeBalance(
        call: ServerCall<ReqT, RespT>,
        headers: Metadata,
        next: ServerCallHandler<ReqT, RespT>,
        subscribe: Boolean
    ): ServerCall.Listener<ReqT> {
        return process(
            call, headers, next,
            EventsBuilder.SubscribeBalance(subscribe) as EventsBuilder.RequestReply<*, ReqT, RespT>
        )
    }

    @Suppress("UNCHECKED_CAST")
    private fun <ReqT : Any, RespT : Any> processSubscribeTxStatus(
        call: ServerCall<ReqT, RespT>,
        headers: Metadata,
        next: ServerCallHandler<ReqT, RespT>
    ): ServerCall.Listener<ReqT> {
        return process(
            call, headers, next,
            EventsBuilder.TxStatus() as EventsBuilder.RequestReply<*, ReqT, RespT>
        )
    }

    @Suppress("UNCHECKED_CAST")
    private fun <ReqT : Any, RespT : Any> processNativeCall(
        call: ServerCall<ReqT, RespT>,
        headers: Metadata,
        next: ServerCallHandler<ReqT, RespT>
    ): ServerCall.Listener<ReqT> {
        return process(
            call, headers, next,
            EventsBuilder.NativeCall(Instant.now()) as EventsBuilder.RequestReply<*, ReqT, RespT>
        )
    }

    @Suppress("UNCHECKED_CAST")
    private fun <ReqT : Any, RespT : Any> processNativeSubscribe(
        call: ServerCall<ReqT, RespT>,
        headers: Metadata,
        next: ServerCallHandler<ReqT, RespT>
    ): ServerCall.Listener<ReqT> {
        return process(
            call, headers, next,
            EventsBuilder.NativeSubscribe(Events.Channel.GRPC) as EventsBuilder.RequestReply<*, ReqT, RespT>
        )
    }

    @Suppress("UNCHECKED_CAST")
    private fun <ReqT : Any, RespT : Any> processDescribe(
        call: ServerCall<ReqT, RespT>,
        headers: Metadata,
        next: ServerCallHandler<ReqT, RespT>
    ): ServerCall.Listener<ReqT> {
        return process(
            call, headers, next,
            EventsBuilder.Describe() as EventsBuilder.RequestReply<*, ReqT, RespT>
        )
    }

    @Suppress("UNCHECKED_CAST")
    private fun <ReqT : Any, RespT : Any> processStatus(
        call: ServerCall<ReqT, RespT>,
        headers: Metadata,
        next: ServerCallHandler<ReqT, RespT>
    ): ServerCall.Listener<ReqT> {
        return process(
            call, headers, next,
            EventsBuilder.Status() as EventsBuilder.RequestReply<*, ReqT, RespT>
        )
    }

    @Suppress("UNCHECKED_CAST")
    private fun <ReqT : Any, RespT : Any> processEstimateFee(
        call: ServerCall<ReqT, RespT>,
        headers: Metadata,
        next: ServerCallHandler<ReqT, RespT>
    ): ServerCall.Listener<ReqT> {
        return process(
            call, headers, next,
            EventsBuilder.EstimateFee() as EventsBuilder.RequestReply<*, ReqT, RespT>
        )
    }

    open class StdCallListener<Req, EB : EventsBuilder.RequestReply<*, Req, *>>(
        val next: ServerCall.Listener<Req>,
        val builder: EB
    ) : ForwardingServerCallListener<Req>() {

        override fun onMessage(message: Req) {
            builder.onRequest(message)
            super.onMessage(message)
        }

        override fun delegate(): ServerCall.Listener<Req> {
            return next
        }
    }

    open class StdCallResponse<ReqT : Any, RespT : Any, EB : EventsBuilder.RequestReply<*, ReqT, RespT>>(
        val next: ServerCall<ReqT, RespT>,
        val builder: EB,
        val accessLogWriter: AccessLogWriter
    ) : ForwardingServerCall<ReqT, RespT>() {

        override fun getMethodDescriptor(): MethodDescriptor<ReqT, RespT> {
            return next.methodDescriptor
        }

        override fun delegate(): ServerCall<ReqT, RespT> {
            return next
        }

        override fun sendMessage(message: RespT) {
            super.sendMessage(message)
            accessLogWriter.submit(
                builder.onReply(message)!!
            )
        }
    }
}
