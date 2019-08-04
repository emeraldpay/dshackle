package io.emeraldpay.dshackle.upstream

import io.infinitape.etherjar.rpc.JacksonRpcConverter
import io.infinitape.etherjar.rpc.RpcException
import org.slf4j.LoggerFactory

abstract class ValueAwareQuorum<T>(
        val jacksonRpcConverter: JacksonRpcConverter,
        val clazz: Class<T>
): CallQuorum {

    private val log = LoggerFactory.getLogger(ValueAwareQuorum::class.java)

    fun extractValue(response: ByteArray, clazz: Class<T>): T? {
        return jacksonRpcConverter.fromJson(response.inputStream(), clazz)
    }

    override fun record(response: ByteArray, upstream: Upstream) {
        try {
            val value = extractValue(response, clazz)
            recordValue(response, value, upstream)
        } catch (e: RpcException) {
            recordError(response, e.rpcMessage, upstream)
        } catch (e: Exception) {
            recordError(response, e.message, upstream)
        }
    }

    abstract fun recordValue(response: ByteArray, responseValue: T?, upstream: Upstream)

    abstract fun recordError(response: ByteArray, errorMessage: String?, upstream: Upstream)

}