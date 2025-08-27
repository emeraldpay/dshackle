package io.emeraldpay.dshackle.monitoring

import com.fasterxml.jackson.databind.util.ByteBufferBackedOutputStream
import io.emeraldpay.dshackle.Global
import org.slf4j.LoggerFactory
import java.nio.BufferOverflowException
import java.nio.ByteBuffer

class LogJsonSerializer<T>(
    initialBuffer: Int = 1024,
) : LogSerializer<T> {
    companion object {
        private val log = LoggerFactory.getLogger(LogJsonSerializer::class.java)
    }

    private val objectMapper = Global.objectMapper
    private var allocateSize = initialBuffer

    override fun apply(t: T): ByteBuffer {
        val buffer = ByteBuffer.allocateDirect(allocateSize)
        return try {
            ByteBufferBackedOutputStream(buffer).use { out ->
                objectMapper.writeValue(out, t)
            }
            buffer.flip()
        } catch (_: BufferOverflowException) {
            // expected size for the buffer is too small, so increase it which will be used for the consequent serializations
            val size = objectMapper.writeValueAsBytes(t).size
            allocateSize = allocateSize.coerceAtLeast(size)
            // and try again with the new size
            apply(t)
        }
    }
}
