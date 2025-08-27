package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.reader.DshackleRpcReader
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleRequest
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleResponse
import io.emeraldpay.etherjar.rpc.RpcException
import io.emeraldpay.etherjar.rpc.RpcResponseError
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import java.util.concurrent.atomic.AtomicReference

class VerifyingReader(
    private val methods: AtomicReference<CallMethods>,
) : DshackleRpcReader {
    companion object {
        private val log = LoggerFactory.getLogger(VerifyingReader::class.java)
    }

    override fun read(key: DshackleRequest): Mono<DshackleResponse> {
        if (!methods.get().isAvailable(key.method)) {
            return Mono.error(RpcException(RpcResponseError.CODE_METHOD_NOT_EXIST, "Unsupported method: ${key.method}"))
        }
        return Mono.empty()
    }
}
