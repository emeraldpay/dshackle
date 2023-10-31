package io.emeraldpay.dshackle.upstream

interface CachingReader : Lifecycle

object NoopCachingReader : CachingReader {
    override fun start() {
    }

    override fun stop() {
    }

    override fun isRunning(): Boolean {
        return true
    }
}
