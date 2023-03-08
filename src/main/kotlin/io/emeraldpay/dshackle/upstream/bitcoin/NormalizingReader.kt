package io.emeraldpay.dshackle.upstream.bitcoin

import io.emeraldpay.dshackle.reader.DshackleRpcReader
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleRequest
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleResponse
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono

class NormalizingReader(
    private val delegate: DshackleRpcReader
) : DshackleRpcReader {

    companion object {
        private val log = LoggerFactory.getLogger(NormalizingReader::class.java)
    }

    override fun read(key: DshackleRequest): Mono<DshackleResponse> {
        return delegate.read(key)
    }
}
