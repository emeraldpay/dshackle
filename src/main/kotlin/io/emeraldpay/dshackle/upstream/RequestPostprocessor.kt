package io.emeraldpay.dshackle.upstream

interface RequestPostprocessor {

    fun onReceive(method: String, params: List<Any>, json: ByteArray)

    class Empty : RequestPostprocessor {
        override fun onReceive(method: String, params: List<Any>, json: ByteArray) {}
    }
}