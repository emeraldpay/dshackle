package io.emeraldpay.dshackle.upstream.bitcoin

import io.emeraldpay.dshackle.upstream.Lifecycle
import org.slf4j.LoggerFactory
import org.zeromq.SocketType
import org.zeromq.ZContext
import org.zeromq.ZMQ
import reactor.core.publisher.Sinks
import java.util.concurrent.Executors
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class ZMQServer(
    val host: String,
    val port: Int,
    val topic: String,
) : Lifecycle {

    companion object {
        private val log = LoggerFactory.getLogger(ZMQServer::class.java)
        private val RECEIVE_TIME_MS = 100
    }

    private val topicId = topic.encodeToByteArray()

    private val runningLock = ReentrantLock()
    private var running = false
    private val sinkPublisher = Executors.newSingleThreadExecutor()
    val sink = Sinks.many()
        .multicast()
        .directBestEffort<ByteArray>()

    fun startInternal() = Runnable {
        log.info("Connecting to ZMQ at $host:$port")
        val context = ZContext()
        val socket: ZMQ.Socket = context.createSocket(SocketType.SUB)
        socket.connect("tcp://$host:$port")
        socket.subscribe(topic)
        socket.receiveTimeOut = RECEIVE_TIME_MS

        while (running && !Thread.currentThread().isInterrupted) {
            val msg = readMessage(socket)
            if (msg != null) {
                // this should not happen, but check just in case
                if (!topicId.contentEquals(msg.id)) {
                    continue
                }
                sinkPublisher.execute {
                    val sent = sink.tryEmitNext(msg.value)
                    if (sent.isFailure && sent != Sinks.EmitResult.FAIL_ZERO_SUBSCRIBER) {
                        log.warn("Failed to notify with $sent")
                    }
                }
            }
        }
        socket.close()
        log.debug("Stopped ZMQ connection to $host:$port")
    }

    fun readMessage(socket: ZMQ.Socket): Message? {
        val id = readOnce(socket)
        val value = readOnce(socket)
        val seq = readOnce(socket)
        if (id != null && value != null && seq != null) {
            return Message(id, value, seq)
        }
        return null
    }

    fun readOnce(socket: ZMQ.Socket): ByteArray? {
        while (running) {
            // blocks until a message is received
            // but usually returns null if nothing received, so have to repeat a request
            val data = socket.recv(0)
            if (data != null) {
                return data
            }
        }
        return null
    }

    override fun start() {
        runningLock.withLock {
            if (running) {
                return
            }
            running = true
        }
        Thread(startInternal()).start()
    }

    override fun stop() {
        runningLock.withLock {
            if (!running) {
                return
            }
            running = false
        }
        log.debug("Stopping ZMQ listener at $host:$port")
        // give some time to the internal thread to receive a message and quit
        Thread.sleep(RECEIVE_TIME_MS.toLong())
    }

    override fun isRunning(): Boolean {
        return running
    }

    // Bitcoind produces messages as something like:
    // | hashblock | <32-byte block hash in Little Endian> | <uint32 sequence number in Little Endian>
    // i.e., it's a triple of values
    data class Message(
        val id: ByteArray,
        val value: ByteArray,
        val sequence: ByteArray,
    )
}
