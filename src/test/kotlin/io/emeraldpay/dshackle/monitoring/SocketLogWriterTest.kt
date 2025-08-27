package io.emeraldpay.dshackle.monitoring

import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.collections.shouldHaveAtLeastSize
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import org.springframework.test.util.TestSocketUtils
import java.net.ServerSocket
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class SocketLogWriterTest :
    ShouldSpec({

        class MockServer(
            val port: Int,
            val expectMessages: Int,
            val messageSize: Int = 3,
        ) {
            val messages = mutableListOf<ByteArray>()
            private val launchAwait = CountDownLatch(1)
            private val stopAwait = CountDownLatch(1)

            fun awaitLaunch(duration: Duration) {
                check(launchAwait.await(duration.toMillis(), TimeUnit.MILLISECONDS)) {
                    "Not launched in $duration"
                }
            }

            fun awaitStop(duration: Duration) {
                check(stopAwait.await(duration.toMillis(), TimeUnit.MILLISECONDS)) {
                    "Not stopped in $duration"
                }
            }

            fun launch() {
                Thread {
                    try {
                        println("Starting Mock TCP server on $port")
                        val socket = ServerSocket(port)
                        println("Started Mock TCP Server")
                        launchAwait.countDown()
                        socket.accept().also {
                            println("Receiving data...")
                            try {
                                repeat(expectMessages) { _ ->
                                    messages.add(it.getInputStream().readNBytes(messageSize))
                                }
                            } catch (t: Throwable) {
                                t.printStackTrace()
                            } finally {
                                it.close()
                                socket.close()
                            }
                            println("Stopped TCP server")
                        }
                    } finally {
                        stopAwait.countDown()
                    }
                }.start()
            }
        }

        should("Send to a socket") {
            val port = TestSocketUtils.findAvailableTcpPort()

            val server = MockServer(port, 1, messageSize = "Hello World!".length + 1)
            server.launch()
            server.awaitLaunch(Duration.ofSeconds(5))

            val writer =
                SocketLogWriter(
                    "localhost",
                    port,
                    CurrentLogWriter.Category.REQUEST,
                    MonitoringTestCommons.defaultSerializer,
                    LogEncodingNewLine(),
                )
            writer.start()
            writer.submit("Hello World!")

            Thread.sleep(250)
            writer.stop()

            server.messages shouldHaveSize 1
            String(server.messages[0]) shouldBe "Hello World!\n"
        }

        should("Reconnect if disconnected") {
            val port = TestSocketUtils.findAvailableTcpPort()

            val server1 = MockServer(port, 10)
            val server2 = MockServer(port, 10)

            server1.launch()
            Thread {
                server1.awaitStop(Duration.ofSeconds(10))
                Thread.sleep(1000)
                server2.launch()
            }.start()

            val writer =
                SocketLogWriter(
                    "localhost",
                    port,
                    CurrentLogWriter.Category.REQUEST,
                    MonitoringTestCommons.defaultSerializer,
                    LogEncodingNewLine(),
                )
            println("Starting...")
            writer.start()

            try {
                println("Sending data...")

                (10 until 20).forEach { i ->
                    Thread.sleep(50)
                    writer.submit(i.toString())
                }
                server1.awaitStop(Duration.ofSeconds(1))
                println("Pause for 1s to avoid sending to the first connection...")
                Thread.sleep(1000)
                server2.awaitLaunch(Duration.ofSeconds(1))
                println("Sending to the second socket")
                (20 until 30).forEach { i ->
                    Thread.sleep(50)
                    writer.submit(i.toString())
                }
            } catch (t: Throwable) {
                t.printStackTrace()
            }
            println("Data sent")
            Thread.sleep(1000)

            println("Stopping...")
            writer.stop()

            server1.messages shouldHaveSize 10
            // unfortunately TCP Client doesn't immediately notice the connection close,
            // so it may send a couple of messages to a wrong TCP channel.
            server2.messages shouldHaveAtLeastSize 5
        }
    })
