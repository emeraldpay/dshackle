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
package io.emeraldpay.dshackle.upstream.rpcclient

import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.reader.StandardRpcReader
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import java.util.function.Function

/**
 * A wrapper around a JsonRpcReader that logs the request through the provided logger
 */
class LoggingJsonRpcReader(
    private val delegate: StandardRpcReader,
    private val logger: (key: JsonRpcRequest) -> Function<Mono<JsonRpcResponse>, Mono<JsonRpcResponse>>,
) : StandardRpcReader {
    companion object {
        private val log = LoggerFactory.getLogger(LoggingJsonRpcReader::class.java)
    }

    var requestContext = Global.monitoring

    override fun read(key: JsonRpcRequest): Mono<JsonRpcResponse> =
        delegate
            .read(key)
            .transform(logger(key))
            .contextWrite(requestContext.ingress.startExecuting())
            // make sure the logger would have the actual request details. b/c if it may be transformed from what was
            // originally made on the client side
            .contextWrite(requestContext.ingress.withRequest(key))
            .contextWrite(requestContext.ingress.ensureInitialized())
}
