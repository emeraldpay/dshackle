package io.emeraldpay.dshackle.test

import io.emeraldpay.dshackle.reader.Reader
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono

class ReaderMock<K, D> : Reader<K, D> {

    companion object {
        private val log = LoggerFactory.getLogger(ReaderMock::class.java)
    }

    private val mapping = mutableMapOf<K, D>()

    fun with(key: K, data: D): ReaderMock<K, D> {
        mapping[key] = data
        return this
    }

    override fun read(key: K): Mono<D> {
        val value = mapping[key]
        if (value == null) {
            log.warn("No mocked value for request: $key")
        }
        return Mono.justOrEmpty(value)
    }
}
