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

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.grpc.*
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

@Service
class AccessHandler(
        @Autowired private val accessLogWriter: AccessLogWriter
) : ServerInterceptor {

    companion object {
        private val log = LoggerFactory.getLogger(AccessHandler::class.java)
    }

    override fun <ReqT : Any, RespT : Any> interceptCall(
            call: ServerCall<ReqT, RespT>,
            headers: Metadata,
            next: ServerCallHandler<ReqT, RespT>): ServerCall.Listener<ReqT> {

        when (val method = call.methodDescriptor.bareMethodName) {
            "SubscribeHead" -> {
                return processSubscribeHead(call, headers, next)
            }
            "NativeCall" -> {
                return processNativeCall(call, headers, next)
            }
            else -> {
                log.trace("unsupported method `{}`", method)
            }
        }

        // continue
        return next.startCall(call, headers)
    }

    @Suppress("UNCHECKED_CAST")
    private fun <ReqT : Any, RespT : Any> processSubscribeHead(
            call: ServerCall<ReqT, RespT>,
            headers: Metadata,
            next: ServerCallHandler<ReqT, RespT>
    ): ServerCall.Listener<ReqT> {
        val builder = EventsBuilder.SubscribeHead()
                .start(headers, call.attributes)
        val callWrapper: ServerCall<ReqT, RespT> = OnSubscribeHeadResponse(
                call as ServerCall<Common.Chain, BlockchainOuterClass.ChainHead>, builder, accessLogWriter) as ServerCall<ReqT, RespT>
        return OnSubscribeHead(
                next.startCall(callWrapper, headers) as ServerCall.Listener<Common.Chain>,
                builder
        ) as ServerCall.Listener<ReqT>
    }

    @Suppress("UNCHECKED_CAST")
    private fun <ReqT : Any, RespT : Any> processNativeCall(
            call: ServerCall<ReqT, RespT>,
            headers: Metadata,
            next: ServerCallHandler<ReqT, RespT>
    ): ServerCall.Listener<ReqT> {
        val builder = EventsBuilder.NativeCall()
                .start(headers, call.attributes)

        val callWrapper: ServerCall<ReqT, RespT> = OnNativeCallResponse(
                call as ServerCall<BlockchainOuterClass.NativeCallRequest, BlockchainOuterClass.NativeCallReplyItem>, builder
        ) as ServerCall<ReqT, RespT>
        return OnNativeCall(
                next.startCall(callWrapper, headers) as ServerCall.Listener<BlockchainOuterClass.NativeCallRequest>,
                builder) { logs ->
            accessLogWriter.submit(logs)
        } as ServerCall.Listener<ReqT>
    }

    class OnSubscribeHead(
            val next: ServerCall.Listener<Common.Chain>,
            val builder: EventsBuilder.SubscribeHead
    ) : ForwardingServerCallListener<Common.Chain>() {

        override fun onMessage(message: Common.Chain) {
            val chainId = message.type.number
            builder.withChain(chainId)
            super.onMessage(message)
        }

        override fun delegate(): ServerCall.Listener<Common.Chain> {
            return next
        }
    }

    class OnNativeCall(
            val next: ServerCall.Listener<BlockchainOuterClass.NativeCallRequest>,
            val builder: EventsBuilder.NativeCall,
            val done: (List<Events.NativeCall>) -> Unit
    ) : ForwardingServerCallListener<BlockchainOuterClass.NativeCallRequest>() {

        override fun onMessage(message: BlockchainOuterClass.NativeCallRequest) {
            val chain = message.chain
            builder.withChain(chain.number)
            message.itemsList.forEach { item ->
                builder.onItem(item)
            }
            super.onMessage(message)
        }

        override fun onCancel() {
            super.onCancel()
            done(builder.build())
        }

        override fun onComplete() {
            super.onComplete()
            done(builder.build())
        }

        override fun delegate(): ServerCall.Listener<BlockchainOuterClass.NativeCallRequest> {
            return next
        }
    }

    abstract class BaseCallResponse<ReqT : Any, RespT : Any>(
            val next: ServerCall<ReqT, RespT>
    ) : ForwardingServerCall<ReqT, RespT>() {
        override fun getMethodDescriptor(): MethodDescriptor<ReqT, RespT> {
            return next.methodDescriptor
        }

        override fun delegate(): ServerCall<ReqT, RespT> {
            return next
        }

        override fun sendMessage(message: RespT) {
            super.sendMessage(message)
        }
    }

    class OnNativeCallResponse(
            next: ServerCall<BlockchainOuterClass.NativeCallRequest, BlockchainOuterClass.NativeCallReplyItem>,
            val builder: EventsBuilder.NativeCall
    ) : BaseCallResponse<BlockchainOuterClass.NativeCallRequest, BlockchainOuterClass.NativeCallReplyItem>(next) {

        override fun sendMessage(message: BlockchainOuterClass.NativeCallReplyItem) {
            builder.onItemReply(message)
            super.sendMessage(message)
        }
    }

    class OnSubscribeHeadResponse(
            next: ServerCall<Common.Chain, BlockchainOuterClass.ChainHead>,
            val builder: EventsBuilder.SubscribeHead,
            val accessLogWriter: AccessLogWriter
    ) : BaseCallResponse<Common.Chain, BlockchainOuterClass.ChainHead>(next) {

        override fun sendMessage(message: BlockchainOuterClass.ChainHead) {
            val event = builder.onReply(message)
            accessLogWriter.submit(event)
            super.sendMessage(message)
        }
    }
}