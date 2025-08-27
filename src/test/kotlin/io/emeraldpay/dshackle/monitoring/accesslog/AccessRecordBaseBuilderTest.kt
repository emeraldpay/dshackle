package io.emeraldpay.dshackle.monitoring.accesslog

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.dshackle.monitoring.Channel
import io.emeraldpay.dshackle.monitoring.record.AccessRecord
import io.grpc.Attributes
import io.grpc.Grpc
import io.grpc.Metadata
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.kotest.matchers.string.shouldHaveLength
import java.net.Inet4Address
import java.net.Inet6Address
import java.net.InetSocketAddress
import java.util.UUID

class AccessRecordBaseBuilderTest :
    ShouldSpec({

        class TestEvent(
            val request: AccessRecord.RequestDetails,
        ) : AccessRecord.Base(UUID.randomUUID(), "TEST", Channel.DSHACKLE)

        class TestEventBuilder :
            RecordBuilder.Base<TestEventBuilder>(UUID.randomUUID()),
            RecordBuilder.RequestReply<TestEvent, BlockchainOuterClass.NativeCallRequest, BlockchainOuterClass.NativeCallReplyItem> {
            override fun getT(): TestEventBuilder = this

            override fun onRequest(msg: BlockchainOuterClass.NativeCallRequest) {
            }

            override fun onReply(msg: BlockchainOuterClass.NativeCallReplyItem): TestEvent = TestEvent(requestDetails)
        }

        should("Parse headers from direct local access") {
            val metadata = Metadata()
            metadata.put(Metadata.Key.of("user-agent", Metadata.ASCII_STRING_MARSHALLER), "grpc-go/1.30.0")
            val attributes =
                Attributes
                    .newBuilder()
                    .set(Grpc.TRANSPORT_ATTR_REMOTE_ADDR, InetSocketAddress(Inet4Address.getByName("127.0.0.1"), 2448))
                    .build()

            val act =
                TestEventBuilder()
                    .also {
                        it.start(metadata, attributes)
                        it.onRequest(BlockchainOuterClass.NativeCallRequest.getDefaultInstance())
                    }.onReply(BlockchainOuterClass.NativeCallReplyItem.getDefaultInstance())

            act.request.remote shouldNotBe null
            with(act.request.remote!!) {
                ips shouldBe listOf("127.0.0.1")
                userAgent shouldBe "grpc-go/1.30.0"
                ip shouldBe "127.0.0.1"
            }
        }

        should("Extract real remote ip") {
            val metadata = Metadata()
            metadata.put(Metadata.Key.of("user-agent", Metadata.ASCII_STRING_MARSHALLER), "grpc-go/1.30.0")
            metadata.put(Metadata.Key.of("x-real-ip", Metadata.ASCII_STRING_MARSHALLER), "30.56.100.15")
            val attributes =
                Attributes
                    .newBuilder()
                    .set(Grpc.TRANSPORT_ATTR_REMOTE_ADDR, InetSocketAddress(Inet4Address.getByName("127.0.0.1"), 2448))
                    .build()

            val act =
                TestEventBuilder()
                    .also {
                        it.start(metadata, attributes)
                        it.onRequest(BlockchainOuterClass.NativeCallRequest.getDefaultInstance())
                    }.onReply(BlockchainOuterClass.NativeCallReplyItem.getDefaultInstance())

            with(act.request.remote!!) {
                ips shouldBe listOf("30.56.100.15", "127.0.0.1")
                ip shouldBe "30.56.100.15"
            }
        }

        should("Ignore remote ip header if already connected from remote") {
            val metadata = Metadata()
            metadata.put(Metadata.Key.of("user-agent", Metadata.ASCII_STRING_MARSHALLER), "grpc-go/1.30.0")
            metadata.put(Metadata.Key.of("x-real-ip", Metadata.ASCII_STRING_MARSHALLER), "192.168.1.1")
            val attributes =
                Attributes
                    .newBuilder()
                    .set(Grpc.TRANSPORT_ATTR_REMOTE_ADDR, InetSocketAddress(Inet4Address.getByName("30.56.100.15"), 2448))
                    .build()

            val act =
                TestEventBuilder()
                    .also {
                        it.start(metadata, attributes)
                        it.onRequest(BlockchainOuterClass.NativeCallRequest.getDefaultInstance())
                    }.onReply(BlockchainOuterClass.NativeCallReplyItem.getDefaultInstance())

            with(act.request.remote!!) {
                ips shouldBe listOf("192.168.1.1", "30.56.100.15")
                userAgent shouldBe "grpc-go/1.30.0"
                ip shouldBe "30.56.100.15"
            }
        }

        should("Ignore invalid ip header") {
            val metadata = Metadata()
            metadata.put(Metadata.Key.of("user-agent", Metadata.ASCII_STRING_MARSHALLER), "grpc-go/1.30.0")
            metadata.put(Metadata.Key.of("x-real-ip", Metadata.ASCII_STRING_MARSHALLER), "271.194.19.1")
            val attributes =
                Attributes
                    .newBuilder()
                    .set(Grpc.TRANSPORT_ATTR_REMOTE_ADDR, InetSocketAddress(Inet4Address.getByName("30.56.100.15"), 2448))
                    .build()

            val act =
                TestEventBuilder()
                    .also {
                        it.start(metadata, attributes)
                        it.onRequest(BlockchainOuterClass.NativeCallRequest.getDefaultInstance())
                    }.onReply(BlockchainOuterClass.NativeCallReplyItem.getDefaultInstance())

            with(act.request.remote!!) {
                ips shouldBe listOf("30.56.100.15")
                userAgent shouldBe "grpc-go/1.30.0"
                ip shouldBe "30.56.100.15"
            }
        }

        should("Ignore host addr in ip header") {
            val metadata = Metadata()
            metadata.put(Metadata.Key.of("user-agent", Metadata.ASCII_STRING_MARSHALLER), "grpc-go/1.30.0")
            metadata.put(Metadata.Key.of("x-real-ip", Metadata.ASCII_STRING_MARSHALLER), "google.com")
            val attributes =
                Attributes
                    .newBuilder()
                    .set(Grpc.TRANSPORT_ATTR_REMOTE_ADDR, InetSocketAddress(Inet4Address.getByName("30.56.100.15"), 2448))
                    .build()

            val act =
                TestEventBuilder()
                    .also {
                        it.start(metadata, attributes)
                        it.onRequest(BlockchainOuterClass.NativeCallRequest.getDefaultInstance())
                    }.onReply(BlockchainOuterClass.NativeCallReplyItem.getDefaultInstance())

            with(act.request.remote!!) {
                ips shouldBe listOf("30.56.100.15")
                userAgent shouldBe "grpc-go/1.30.0"
                ip shouldBe "30.56.100.15"
            }
        }

        should("Extract ipv6 addresses") {
            val metadata = Metadata()
            metadata.put(Metadata.Key.of("user-agent", Metadata.ASCII_STRING_MARSHALLER), "grpc-go/1.30.0")
            metadata.put(Metadata.Key.of("x-real-ip", Metadata.ASCII_STRING_MARSHALLER), "2001:0db8:0000:0000:0000:ff00:0042:8329")
            val attributes =
                Attributes
                    .newBuilder()
                    .set(Grpc.TRANSPORT_ATTR_REMOTE_ADDR, InetSocketAddress(Inet6Address.getByName("::1"), 2448))
                    .build()

            val act =
                TestEventBuilder()
                    .also {
                        it.start(metadata, attributes)
                        it.onRequest(BlockchainOuterClass.NativeCallRequest.getDefaultInstance())
                    }.onReply(BlockchainOuterClass.NativeCallReplyItem.getDefaultInstance())

            with(act.request.remote!!) {
                ips shouldBe listOf("2001:db8:0:0:0:ff00:42:8329", "0:0:0:0:0:0:0:1")
                ip shouldBe "2001:db8:0:0:0:ff00:42:8329"
            }
        }

        should("Clean up user agent") {
            val metadata = Metadata()
            metadata.put(Metadata.Key.of("user-agent", Metadata.ASCII_STRING_MARSHALLER), "grpc-go/1.30.0\nxss\n\r")
            val attributes =
                Attributes
                    .newBuilder()
                    .set(Grpc.TRANSPORT_ATTR_REMOTE_ADDR, InetSocketAddress(Inet4Address.getByName("30.56.100.15"), 2448))
                    .build()

            val act =
                TestEventBuilder()
                    .also {
                        it.start(metadata, attributes)
                        it.onRequest(BlockchainOuterClass.NativeCallRequest.getDefaultInstance())
                    }.onReply(BlockchainOuterClass.NativeCallReplyItem.getDefaultInstance())

            with(act.request.remote!!) {
                userAgent shouldBe "grpc-go/1.30.0 xss"
            }
        }

        should("Truncate up user agent to 128 characters max") {
            val metadata = Metadata()
            metadata.put(
                Metadata.Key.of("user-agent", Metadata.ASCII_STRING_MARSHALLER),
                "0123456_1_0123456_2_0123456_3_0123456_4_0123456_5_0123456_6_0123456_7_0123456_8_0123456_9_0123456_0_0123456_1_0123456_2_0123456_3_0123456_4_0123456_5",
            )
            val attributes =
                Attributes
                    .newBuilder()
                    .set(Grpc.TRANSPORT_ATTR_REMOTE_ADDR, InetSocketAddress(Inet4Address.getByName("30.56.100.15"), 2448))
                    .build()

            val act =
                TestEventBuilder()
                    .also {
                        it.start(metadata, attributes)
                        it.onRequest(BlockchainOuterClass.NativeCallRequest.getDefaultInstance())
                    }.onReply(BlockchainOuterClass.NativeCallReplyItem.getDefaultInstance())

            with(act.request.remote!!) {
                userAgent shouldHaveLength 128
                userAgent shouldBe
                    "0123456_1_0123456_2_0123456_3_0123456_4_0123456_5_0123456_6_0123456_7_0123456_8_0123456_9_0123456_0_0123456_1_0123456_2_0123456_"
            }
        }
    })
