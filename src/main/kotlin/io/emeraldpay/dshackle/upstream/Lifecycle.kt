package io.emeraldpay.dshackle.upstream

interface Lifecycle {
    fun start()
    fun stop()
    fun isRunning(): Boolean
}
