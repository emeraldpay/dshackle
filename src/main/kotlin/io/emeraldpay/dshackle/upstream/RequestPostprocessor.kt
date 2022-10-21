package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.reader.JsonRpcReader
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import reactor.core.publisher.Mono

interface RequestPostprocessor {

    fun onReceive(method: String, params: List<Any?>, json: ByteArray)

    class Empty : RequestPostprocessor {
        override fun onReceive(method: String, params: List<Any?>, json: ByteArray) {}
    }

    companion object {
        fun wrap(
            reader: JsonRpcReader,
            processor: RequestPostprocessor
        ): JsonRpcReader {
            return Wrapper(reader, processor)
        }
    }

    class Wrapper(
        private val reader: JsonRpcReader,
        private val processor: RequestPostprocessor
    ) : JsonRpcReader {

        override fun read(key: JsonRpcRequest): Mono<JsonRpcResponse> {
            return reader.read(key)
                .doOnNext {
                    if (it.hasResult()) {
                        val result = it.getResult()
                        processor.onReceive(key.method, key.params, result)
                    }
                }
        }
    }
}
