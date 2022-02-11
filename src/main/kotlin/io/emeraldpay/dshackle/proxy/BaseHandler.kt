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
package io.emeraldpay.dshackle.proxy

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.monitoring.accesslog.AccessHandlerHttp
import io.emeraldpay.dshackle.rpc.NativeCall
import io.emeraldpay.grpc.Chain
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.util.concurrent.TimeUnit

abstract class BaseHandler(
    private val writeRpcJson: WriteRpcJson,
    private val nativeCall: NativeCall,
    private val requestMetrics: ProxyServer.RequestMetricsFactory,
) {

    companion object {
        private val log = LoggerFactory.getLogger(BaseHandler::class.java)
    }

    fun execute(chain: Chain, call: ProxyCall, handler: AccessHandlerHttp.RequestHandler): Publisher<String> {
        // return empty response for empty request
        if (call.items.isEmpty()) {
            return if (call.type == ProxyCall.RpcType.BATCH) {
                Mono.just("[]")
            } else {
                Mono.just("")
            }
        }
        val jsons = execute(chain, call.items, handler)
            .transform(writeRpcJson.toJsons(call))
        return if (call.type == ProxyCall.RpcType.SINGLE) {
            jsons.next()
        } else {
            jsons.transform(writeRpcJson.asArray())
        }
    }

    fun execute(chain: Chain, items: List<BlockchainOuterClass.NativeCallItem>, handler: AccessHandlerHttp.RequestHandler): Flux<NativeCall.CallResult> {
        val startTime = System.currentTimeMillis()
        // during the execution we know only ID of the call, so we use it to find the origin call and associated metrics
        val metricById = { id: Int ->
            items.find { it.id == id }?.let { item ->
                requestMetrics.get(chain, item.method)
            }
        }
        val request = BlockchainOuterClass.NativeCallRequest.newBuilder()
            .setChain(Common.ChainRef.forNumber(chain.id))
            .addAllItems(items)
            .build()
        handler.onRequest(request)
        return nativeCall
            .nativeCallResult(Mono.just(request))
            .doOnNext {
                metricById(it.id)?.let { metrics ->
                    metrics.requestMetric.increment()
                    metrics.callMetric.record(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS)
                    if (it.isError()) {
                        metrics.errorMetric.increment()
                    }
                }
                handler.onResponse(it)
            }
            .doOnError {
                // when error happened the whole flux is stopped and no result is produced, so we should mark all the requests as failed
                items.forEach { item ->
                    requestMetrics.get(chain, item.method).failMetric.increment()
                }
            }
    }
}
