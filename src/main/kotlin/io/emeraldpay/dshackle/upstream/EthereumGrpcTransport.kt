/**
 * Copyright (c) 2019 ETCDEV GmbH
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
package io.emeraldpay.dshackle.upstream

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.protobuf.ByteString
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.api.proto.ReactorBlockchainGrpc
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.rpc.Batch
import io.infinitape.etherjar.rpc.JacksonRpcConverter
import io.infinitape.etherjar.rpc.RpcException
import io.infinitape.etherjar.rpc.transport.BatchStatus
import io.infinitape.etherjar.rpc.transport.RpcTransport
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.toFlux
import reactor.util.function.Tuple3
import reactor.util.function.Tuples
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.function.Function

class EthereumGrpcTransport(
        private val chainRef: Common.ChainRef,
        private val selector: BlockchainOuterClass.Selector?,
        private val client: ReactorBlockchainGrpc.ReactorBlockchainStub,
        private val objectMapper: ObjectMapper
): RpcTransport {

    private val jacksonRpcConverter = JacksonRpcConverter(objectMapper)

    constructor(
            chain: Chain,
            client: ReactorBlockchainGrpc.ReactorBlockchainStub,
            objectMapper: ObjectMapper
    ) : this(Common.ChainRef.forNumber(chain.id), Selector.EmptyMatcher().asProto(), client, objectMapper)

    fun withMatcher(matcher: Selector.Matcher): EthereumGrpcTransport {
        if (matcher is Selector.EmptyMatcher && selector == null) {
            return this
        }
        return EthereumGrpcTransport(chainRef, matcher.asProto(), client, objectMapper)
    }

    override fun close() {
    }

    private fun replyProcessor(mapping: HashMap<Int, Batch.BatchItem<Any, Any>>): Function<BlockchainOuterClass.NativeCallReplyItem, Boolean> {
        return Function { resp ->
            val id = resp.id
            val bi = mapping.remove(id)
            if (bi != null) {
                if (resp.succeed) {
                    try {
                        val rpcResp = jacksonRpcConverter.fromJson(resp.payload.toByteArray().inputStream(), bi.call.jsonType, Int::class.java)
                        bi.onComplete(rpcResp)
                        return@Function true
                    } catch (e: RpcException) {
                        bi.onError(e)
                    }
                } else {
                    bi.onError(RpcException(-32603, resp.error.toString()))
                }
            }
            false
        }
    }

    private val sumStatus = { t: Tuple3<Int, Int, Int>, ok: Boolean ->
        if (ok) Tuples.of(t.t1 + 1, t.t2, t.t3 + 1)
        else    Tuples.of(t.t1, t.t2 + 1, t.t3 + 1)
    }

    private val asStatus = Function<Tuple3<Int, Int, Int>, BatchStatus> {
        BatchStatus.newBuilder()
                .withSucceed(it.t1)
                .withFailed(it.t2)
                .withTotal(it.t3)
                .build()
    }

    fun prepareMapping(items: List<Batch.BatchItem<out Any, out Any>>, req: BlockchainOuterClass.NativeCallRequest.Builder): HashMap<Int, Batch.BatchItem<Any, Any>> {
        val mapping = HashMap<Int, Batch.BatchItem<Any, Any>>()
        var seq: Int = 0
        items.forEach { bi ->
            val id = seq++
            mapping[id] = bi as Batch.BatchItem<Any, Any>
            val call = bi.call
            val params = objectMapper.writeValueAsBytes(call.params)
            val nativeCallItem = BlockchainOuterClass.NativeCallItem.newBuilder()
                    .setId(id)
                    .setMethod(call.method)
                    .setPayload(ByteString.copyFrom(params))
                    .build()
            req.addItems(nativeCallItem)
        }
        return mapping
    }

    override fun execute(items: List<Batch.BatchItem<out Any, out Any>>): CompletableFuture<BatchStatus> {
        val req = BlockchainOuterClass.NativeCallRequest.newBuilder()
                .setChain(chainRef)
        if (selector != null) {
            req.setSelector(selector)
        }
        val mapping = prepareMapping(items, req)
        return client.nativeCall(req.build())
                .map(replyProcessor(mapping))
                .reduce(Tuples.of(0, 0, 0), sumStatus)
                .map(asStatus)
                .doFinally {
                    mapping.values.forEach { bi ->
                        bi.onError(RpcException(-32603, "RPC response not received"))
                    }
                }
                .timeout(Duration.ofSeconds(15))
                .toFuture()
    }
}