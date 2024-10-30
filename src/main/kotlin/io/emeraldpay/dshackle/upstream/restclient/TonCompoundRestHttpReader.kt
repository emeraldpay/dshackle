package io.emeraldpay.dshackle.upstream.restclient

import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.ChainResponse
import io.emeraldpay.dshackle.upstream.HttpReader
import reactor.core.publisher.Mono

class TonCompoundRestHttpReader(
    private val restReader: HttpReader,
    private val restReaderV3: HttpReader,
) : HttpReader() {

    override fun read(key: ChainRequest): Mono<ChainResponse> {
        if (key.method.contains("v3")) {
            return restReaderV3.read(key)
        }
        return restReader.read(key)
    }

    override fun internalRead(key: ChainRequest): Mono<ChainResponse> {
        return Mono.empty()
    }

    override fun onStop() {
        restReader.onStop()
        restReaderV3.onStop()
    }
}
