package io.emeraldpay.dshackle.monitoring

import org.apache.commons.codec.binary.Hex
import java.lang.RuntimeException
import java.nio.ByteBuffer

class MonitoringTestCommons {
    companion object {
        val defaultSerializer: LogSerializer<String> =
            object : LogSerializer<String> {
                override fun apply(t: String): ByteBuffer = ByteBuffer.wrap(t.toByteArray())
            }
        val failSerializer: LogSerializer<String> =
            object : LogSerializer<String> {
                override fun apply(t: String): ByteBuffer {
                    if (t == "fail") {
                        throw RuntimeException()
                    }
                    return ByteBuffer.wrap(t.toByteArray())
                }
            }

        fun bufferToString(buffer: ByteBuffer): String {
            val bytes = ByteArray(buffer.remaining())
            buffer.get(bytes)
            return String(bytes)
        }

        fun bufferToHex(buffer: ByteBuffer): String {
            val bytes = ByteArray(buffer.remaining())
            buffer.get(bytes)
            return Hex.encodeHexString(bytes)
        }
    }
}
