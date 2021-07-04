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

        return when (val method = call.methodDescriptor.bareMethodName) {
            "SubscribeHead" -> processSubscribeHead(call, headers, next)
            "SubscribeBalance" -> processSubscribeBalance(call, headers, next, true)
            "SubscribeTxStatus" -> processSubscribeTxStatus(call, headers, next)
            "GetBalance" -> processSubscribeBalance(call, headers, next, false)
            "NativeCall" -> processNativeCall(call, headers, next)
            "Describe" -> processDescribe(call, headers, next)
            else -> {
                log.trace("unsupported method `{}`", method)
                next.startCall(call, headers)
            }
        }
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
    private fun <ReqT : Any, RespT : Any> processSubscribeBalance(
            call: ServerCall<ReqT, RespT>,
            headers: Metadata,
            next: ServerCallHandler<ReqT, RespT>,
            subscribe: Boolean
    ): ServerCall.Listener<ReqT> {
        val builder = EventsBuilder.SubscribeBalance(subscribe)
                .start(headers, call.attributes)
        val callWrapper: ServerCall<ReqT, RespT> = OnSubscribeBalanceResponse(
                call as ServerCall<BlockchainOuterClass.BalanceRequest, BlockchainOuterClass.AddressBalance>, builder, accessLogWriter) as ServerCall<ReqT, RespT>
        return OnSubscribeBalance(
                next.startCall(callWrapper, headers) as ServerCall.Listener<BlockchainOuterClass.BalanceRequest>,
                builder
        ) as ServerCall.Listener<ReqT>
    }

    @Suppress("UNCHECKED_CAST")
    private fun <ReqT : Any, RespT : Any> processSubscribeTxStatus(
            call: ServerCall<ReqT, RespT>,
            headers: Metadata,
            next: ServerCallHandler<ReqT, RespT>
    ): ServerCall.Listener<ReqT> {
        val builder = EventsBuilder.TxStatus()
                .start(headers, call.attributes)
        val callWrapper: ServerCall<ReqT, RespT> = OnTxStatusResponse(
                call as ServerCall<BlockchainOuterClass.TxStatusRequest, BlockchainOuterClass.TxStatus>, builder, accessLogWriter) as ServerCall<ReqT, RespT>
        return OnSubscribeTxStatus(
                next.startCall(callWrapper, headers) as ServerCall.Listener<BlockchainOuterClass.TxStatusRequest>,
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

    @Suppress("UNCHECKED_CAST")
    private fun <ReqT : Any, RespT : Any> processDescribe(
            call: ServerCall<ReqT, RespT>,
            headers: Metadata,
            next: ServerCallHandler<ReqT, RespT>
    ): ServerCall.Listener<ReqT> {
        val builder = EventsBuilder.Describe()
                .start(headers, call.attributes)

        val callWrapper: ServerCall<ReqT, RespT> = OnDescribeResponse(
                call as ServerCall<BlockchainOuterClass.DescribeRequest, BlockchainOuterClass.DescribeResponse>,
                builder,
                accessLogWriter
        ) as ServerCall<ReqT, RespT>
        return OnDescribeRequest(
                next.startCall(callWrapper, headers) as ServerCall.Listener<BlockchainOuterClass.DescribeRequest>,
                builder) as ServerCall.Listener<ReqT>
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

    class OnSubscribeBalance(
            val next: ServerCall.Listener<BlockchainOuterClass.BalanceRequest>,
            val builder: EventsBuilder.SubscribeBalance
    ) : ForwardingServerCallListener<BlockchainOuterClass.BalanceRequest>() {

        override fun onMessage(message: BlockchainOuterClass.BalanceRequest) {
            builder.withRequest(message)
            super.onMessage(message)
        }

        override fun delegate(): ServerCall.Listener<BlockchainOuterClass.BalanceRequest> {
            return next
        }
    }

    class OnSubscribeTxStatus(
            val next: ServerCall.Listener<BlockchainOuterClass.TxStatusRequest>,
            val builder: EventsBuilder.TxStatus
    ) : ForwardingServerCallListener<BlockchainOuterClass.TxStatusRequest>() {

        override fun onMessage(message: BlockchainOuterClass.TxStatusRequest) {
            builder.withRequest(message)
            super.onMessage(message)
        }

        override fun delegate(): ServerCall.Listener<BlockchainOuterClass.TxStatusRequest> {
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

    class OnDescribeRequest(
            val next: ServerCall.Listener<BlockchainOuterClass.DescribeRequest>,
            val builder: EventsBuilder.Describe
    ) : ForwardingServerCallListener<BlockchainOuterClass.DescribeRequest>() {

        override fun onMessage(message: BlockchainOuterClass.DescribeRequest) {
            super.onMessage(message)
        }

        override fun delegate(): ServerCall.Listener<BlockchainOuterClass.DescribeRequest> {
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

    class OnSubscribeBalanceResponse(
            next: ServerCall<BlockchainOuterClass.BalanceRequest, BlockchainOuterClass.AddressBalance>,
            val builder: EventsBuilder.SubscribeBalance,
            val accessLogWriter: AccessLogWriter
    ) : BaseCallResponse<BlockchainOuterClass.BalanceRequest, BlockchainOuterClass.AddressBalance>(next) {

        override fun sendMessage(message: BlockchainOuterClass.AddressBalance) {
            val event = builder.onReply(message)
            accessLogWriter.submit(event)
            super.sendMessage(message)
        }
    }

    class OnTxStatusResponse(
            next: ServerCall<BlockchainOuterClass.TxStatusRequest, BlockchainOuterClass.TxStatus>,
            val builder: EventsBuilder.TxStatus,
            val accessLogWriter: AccessLogWriter
    ) : BaseCallResponse<BlockchainOuterClass.TxStatusRequest, BlockchainOuterClass.TxStatus>(next) {

        override fun sendMessage(message: BlockchainOuterClass.TxStatus) {
            val event = builder.onReply(message)
            accessLogWriter.submit(event)
            super.sendMessage(message)
        }
    }

    class OnDescribeResponse(
            next: ServerCall<BlockchainOuterClass.DescribeRequest, BlockchainOuterClass.DescribeResponse>,
            val builder: EventsBuilder.Describe,
            val accessLogWriter: AccessLogWriter
    ) : BaseCallResponse<BlockchainOuterClass.DescribeRequest, BlockchainOuterClass.DescribeResponse>(next) {

        override fun sendMessage(message: BlockchainOuterClass.DescribeResponse) {
            val event = builder.onReply()
            accessLogWriter.submit(event)
            super.sendMessage(message)
        }
    }
}