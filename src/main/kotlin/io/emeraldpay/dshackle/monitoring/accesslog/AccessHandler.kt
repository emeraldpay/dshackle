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
import io.emeraldpay.dshackle.Global
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
            "NativeCall" -> {
                val builder = Events.NativeCallBuilder()
                return OnNativeCall<ReqT, RespT>(
                        next.startCall(OnNativeCallResponse(call, builder), headers),
                        builder) { logs ->
                    accessLogWriter.submit(logs)
                }
            }
            else -> {
                log.trace("unsupported method `{}`", method)
            }
        }

        // continue
        return next.startCall(call, headers)
    }


    class OnNativeCall<ReqT : Any, RespT : Any>(
            val next: ServerCall.Listener<ReqT>,
            val builder: Events.NativeCallBuilder,
            val done: (List<Events.NativeCall>) -> Unit
    ) : ForwardingServerCallListener<ReqT>() {

        override fun onMessage(message: ReqT) {
            if (message is BlockchainOuterClass.NativeCallRequest) {
                val chain = message.chain
                builder.withChain(chain.number)
                message.itemsList.forEach { item ->
                    builder.onItem(item)
                }
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

        override fun delegate(): ServerCall.Listener<ReqT> {
            return next
        }
    }

    class OnNativeCallResponse<ReqT : Any, RespT : Any>(
            val next: ServerCall<ReqT, RespT>,
            val builder: Events.NativeCallBuilder
    ) : ForwardingServerCall<ReqT, RespT>() {

        override fun getMethodDescriptor(): MethodDescriptor<ReqT, RespT> {
            return next.methodDescriptor
        }

        override fun delegate(): ServerCall<ReqT, RespT> {
            return next
        }

        override fun sendMessage(message: RespT) {
            if (message is BlockchainOuterClass.NativeCallReplyItem) {
                builder.onItemReply(message)
            }
            super.sendMessage(message)
        }
    }
}