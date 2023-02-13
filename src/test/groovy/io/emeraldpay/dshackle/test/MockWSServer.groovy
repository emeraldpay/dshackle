package io.emeraldpay.dshackle.test

import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream
import org.java_websocket.WebSocket
import org.java_websocket.handshake.ClientHandshake
import org.java_websocket.server.WebSocketServer

import java.nio.ByteBuffer
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter

class MockWSServer extends WebSocketServer {

    private def format = DateTimeFormatter.ofPattern("HH:mm:ss.SSS")

    List<ReceivedMessage> received = []
    private WebSocket conn

    private String next

    MockWSServer(int port) {
        super(new InetSocketAddress("127.0.0.1", port))
    }

    void log(String msg) {
        println(format.format(Instant.now().atZone(ZoneId.systemDefault())) + " MOCKWS: " + msg)
    }

    void reply(String message) {
        log(">> $message")
        if (conn == null) {
            log("MOCKWS: ERROR, no active connection")
        }
        conn.send(message)
    }

    void onNextReply(String message) {
        next = message
    }

    @Override
    void onOpen(WebSocket conn, ClientHandshake handshake) {
        this.conn = conn
        log("Opened connection from ${conn.remoteSocketAddress}")
    }

    @Override
    void onClose(WebSocket conn, int code, String reason, boolean remote) {
        this.conn = null
        log("Connection closed, code ${code} with msg '${reason}' ${remote ? 'by remote' : 'by server'}")
    }

    @Override
    void onMessage(WebSocket conn, String message) {
        log("<< $message")
        received.add(new ReceivedMessage(message))
        if (next != null) {
            reply(next)
            next = null
        }
    }

    @Override
    void onMessage(WebSocket conn, ByteBuffer message) {
        onMessage(conn, new ByteBufferBackedInputStream(message).text)
    }

    @Override
    void onError(WebSocket conn, Exception ex) {
        log("ERROR, $ex.message")
        received.add(new ReceivedMessage("Err: ${ex.message}"))
    }

    @Override
    void onStart() {
        log("Server started")
    }

    class ReceivedMessage {
        final String value

        ReceivedMessage(String value) {
            this.value = value
        }
    }
}
