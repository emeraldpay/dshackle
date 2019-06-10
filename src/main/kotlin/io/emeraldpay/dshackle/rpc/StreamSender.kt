package io.emeraldpay.dshackle.rpc

import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.grpc.stub.StreamObserver
import org.slf4j.LoggerFactory

class StreamSender<T>(val stream: StreamObserver<T>) {

    private val log = LoggerFactory.getLogger(StreamSender::class.java)

    fun send(value: T): Boolean {
        try {
            stream.onNext(value)
            return true
        } catch (e: StatusRuntimeException) {
            if (e.status.code != Status.CANCELLED.code) {
                log.warn("Channel errored with ${e.status}: ${e.message}")
            }
        } catch (e: Exception) {
            log.warn("Channel errored with ${e.javaClass.name}: ${e.message}")
            stream.onError(e)
        }
        return false
    }
}