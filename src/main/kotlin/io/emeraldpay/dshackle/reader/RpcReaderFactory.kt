package io.emeraldpay.dshackle.reader

import io.emeraldpay.dshackle.quorum.CallQuorum
import io.emeraldpay.dshackle.quorum.QuorumRpcReader
import io.emeraldpay.dshackle.reader.RpcReader.Result
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.ethereum.rpc.RpcException
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcError
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcException
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.dshackle.upstream.rpcclient.stream.Chunk
import io.emeraldpay.dshackle.upstream.signature.ResponseSigner
import org.springframework.cloud.sleuth.Tracer
import reactor.core.publisher.Flux
import java.util.concurrent.atomic.AtomicInteger

abstract class RpcReader(
    private val signer: ResponseSigner?,
) : Reader<JsonRpcRequest, Result> {
    abstract fun attempts(): AtomicInteger

    protected fun getError(key: JsonRpcRequest, err: Throwable) =
        when (err) {
            is RpcException -> JsonRpcException.from(err)
            is JsonRpcException -> err
            else -> JsonRpcException(
                JsonRpcResponse.NumberId(key.id),
                JsonRpcError(-32603, "Unhandled internal error: ${err.javaClass}: ${err.message}"),
            )
        }

    protected fun handleError(error: JsonRpcError?, id: Int, resolvedBy: String?) =
        error?.asException(JsonRpcResponse.NumberId(id), resolvedBy)
            ?: JsonRpcException(JsonRpcResponse.NumberId(id), JsonRpcError(-32603, "Unhandled Upstream error"), resolvedBy)

    protected fun getSignature(key: JsonRpcRequest, response: JsonRpcResponse, upstreamId: String) =
        response.providedSignature
            ?: if (key.nonce != null) {
                signer?.sign(key.nonce, response.getResult(), upstreamId)
            } else {
                null
            }

    class Result(
        val value: ByteArray,
        val signature: ResponseSigner.Signature?,
        val quorum: Int,
        val resolvedBy: Upstream?,
        val stream: Flux<Chunk>?,
    )
}

interface RpcReaderFactory {

    companion object {
        fun default(): RpcReaderFactory {
            return Default()
        }
    }

    fun create(data: RpcReaderData): RpcReader

    class Default : RpcReaderFactory {
        override fun create(data: RpcReaderData): RpcReader {
            if (data.method == "eth_sendRawTransaction" || data.method == "eth_getTransactionCount") {
                return BroadcastReader(data.multistream.getAll(), data.matcher, data.signer, data.quorum, data.tracer)
            }
            val apis = data.multistream.getApiSource(data.matcher)
            return QuorumRpcReader(apis, data.quorum, data.signer, data.tracer)
        }
    }

    data class RpcReaderData(
        val multistream: Multistream,
        val method: String,
        val matcher: Selector.Matcher,
        val quorum: CallQuorum,
        val signer: ResponseSigner?,
        val tracer: Tracer,
    )
}
