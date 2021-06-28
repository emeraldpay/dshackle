/**
 * Copyright (c) 2021 EmeraldPay, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.emeraldpay.dshackle.monitoring.accesslog

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.grpc.Chain
import io.grpc.Attributes
import io.grpc.Grpc
import io.grpc.Metadata
import spock.lang.Specification

class EventsBaseBuilderSpec extends Specification {

    def "Parse headers from direct local access"() {
        setup:
        def metadata = new Metadata()
        metadata.put(Metadata.Key.of("user-agent", Metadata.ASCII_STRING_MARSHALLER), "grpc-go/1.30.0")
        def attributes = Attributes.newBuilder()
                .set(Grpc.TRANSPORT_ATTR_REMOTE_ADDR, new InetSocketAddress(Inet4Address.getByName("127.0.0.1"), 2448))
                .build()
        when:
        def act = new EventsBuilder.NativeCall()
                .start(metadata, attributes)
                .withChain(Chain.ETHEREUM.id)
                .onItem(BlockchainOuterClass.NativeCallItem.getDefaultInstance())
                .build()
        then:
        act.size() == 1
        with(act[0]) {
            it.request != null
            it.request.remote != null
            with(it.request.remote) {
                ips == ["127.0.0.1"]
                userAgent == "grpc-go/1.30.0"
                ip == "127.0.0.1"
            }
        }
    }

    def "Extracts real remote ip"() {
        setup:
        def metadata = new Metadata()
        metadata.put(Metadata.Key.of("user-agent", Metadata.ASCII_STRING_MARSHALLER), "grpc-go/1.30.0")
        metadata.put(Metadata.Key.of("x-real-ip", Metadata.ASCII_STRING_MARSHALLER), "30.56.100.15")
        def attributes = Attributes.newBuilder()
                .set(Grpc.TRANSPORT_ATTR_REMOTE_ADDR, new InetSocketAddress(Inet4Address.getByName("127.0.0.1"), 2448))
                .build()
        when:
        def act = new EventsBuilder.NativeCall()
                .start(metadata, attributes)
                .withChain(Chain.ETHEREUM.id)
                .onItem(BlockchainOuterClass.NativeCallItem.getDefaultInstance())
                .build()
        then:
        act.size() == 1
        with(act[0].request.remote) {
            ips == ["30.56.100.15", "127.0.0.1"]
            ip == "30.56.100.15"
        }
    }

    def "Ignores remote ip header if already connected from remote"() {
        setup:
        def metadata = new Metadata()
        metadata.put(Metadata.Key.of("user-agent", Metadata.ASCII_STRING_MARSHALLER), "grpc-go/1.30.0")
        metadata.put(Metadata.Key.of("x-real-ip", Metadata.ASCII_STRING_MARSHALLER), "192.168.1.1")
        def attributes = Attributes.newBuilder()
                .set(Grpc.TRANSPORT_ATTR_REMOTE_ADDR, new InetSocketAddress(Inet4Address.getByName("30.56.100.15"), 2448))
                .build()
        when:
        def act = new EventsBuilder.NativeCall()
                .start(metadata, attributes)
                .withChain(Chain.ETHEREUM.id)
                .onItem(BlockchainOuterClass.NativeCallItem.getDefaultInstance())
                .build()
        then:
        act.size() == 1
        with(act[0].request.remote) {
            ips == ["192.168.1.1", "30.56.100.15"]
            userAgent == "grpc-go/1.30.0"
            ip == "30.56.100.15"
        }
    }

    def "Ignores invalid ip header"() {
        setup:
        def metadata = new Metadata()
        metadata.put(Metadata.Key.of("user-agent", Metadata.ASCII_STRING_MARSHALLER), "grpc-go/1.30.0")
        metadata.put(Metadata.Key.of("x-real-ip", Metadata.ASCII_STRING_MARSHALLER), "271.194.19.1")
        def attributes = Attributes.newBuilder()
                .set(Grpc.TRANSPORT_ATTR_REMOTE_ADDR, new InetSocketAddress(Inet4Address.getByName("30.56.100.15"), 2448))
                .build()
        when:
        def act = new EventsBuilder.NativeCall()
                .start(metadata, attributes)
                .withChain(Chain.ETHEREUM.id)
                .onItem(BlockchainOuterClass.NativeCallItem.getDefaultInstance())
                .build()
        then:
        act.size() == 1
        with(act[0].request.remote) {
            ips == ["30.56.100.15"]
            userAgent == "grpc-go/1.30.0"
            ip == "30.56.100.15"
        }
    }

    def "Ignores host addr in ip header"() {
        setup:
        def metadata = new Metadata()
        metadata.put(Metadata.Key.of("user-agent", Metadata.ASCII_STRING_MARSHALLER), "grpc-go/1.30.0")
        metadata.put(Metadata.Key.of("x-real-ip", Metadata.ASCII_STRING_MARSHALLER), "google.com")
        def attributes = Attributes.newBuilder()
                .set(Grpc.TRANSPORT_ATTR_REMOTE_ADDR, new InetSocketAddress(Inet4Address.getByName("30.56.100.15"), 2448))
                .build()
        when:
        def act = new EventsBuilder.NativeCall()
                .start(metadata, attributes)
                .withChain(Chain.ETHEREUM.id)
                .onItem(BlockchainOuterClass.NativeCallItem.getDefaultInstance())
                .build()
        then:
        act.size() == 1
        with(act[0].request.remote) {
            ips == ["30.56.100.15"]
            userAgent == "grpc-go/1.30.0"
            ip == "30.56.100.15"
        }
    }

    def "Extracts ipv6 addresses"() {
        setup:
        def metadata = new Metadata()
        metadata.put(Metadata.Key.of("user-agent", Metadata.ASCII_STRING_MARSHALLER), "grpc-go/1.30.0")
        metadata.put(Metadata.Key.of("x-real-ip", Metadata.ASCII_STRING_MARSHALLER), "2001:0db8:0000:0000:0000:ff00:0042:8329")
        def attributes = Attributes.newBuilder()
                .set(Grpc.TRANSPORT_ATTR_REMOTE_ADDR, new InetSocketAddress(Inet6Address.getByName("::1"), 2448))
                .build()
        when:
        def act = new EventsBuilder.NativeCall()
                .start(metadata, attributes)
                .withChain(Chain.ETHEREUM.id)
                .onItem(BlockchainOuterClass.NativeCallItem.getDefaultInstance())
                .build()
        then:
        act.size() == 1
        with(act[0].request.remote) {
            ips == ["2001:db8:0:0:0:ff00:42:8329", "0:0:0:0:0:0:0:1"]
            ip == "2001:db8:0:0:0:ff00:42:8329"
        }
    }

    def "Cleans up user agent"() {
        setup:
        def metadata = new Metadata()
        metadata.put(Metadata.Key.of("user-agent", Metadata.ASCII_STRING_MARSHALLER), "grpc-go/1.30.0\nxss\n\r")
        def attributes = Attributes.newBuilder()
                .set(Grpc.TRANSPORT_ATTR_REMOTE_ADDR, new InetSocketAddress(Inet4Address.getByName("30.56.100.15"), 2448))
                .build()
        when:
        def act = new EventsBuilder.NativeCall()
                .start(metadata, attributes)
                .withChain(Chain.ETHEREUM.id)
                .onItem(BlockchainOuterClass.NativeCallItem.getDefaultInstance())
                .build()
        then:
        act.size() == 1
        with(act[0].request.remote) {
            userAgent == "grpc-go/1.30.0 xss"
        }
    }

    def "Truncates up user agent to 128 characters max"() {
        setup:
        def metadata = new Metadata()
        metadata.put(Metadata.Key.of("user-agent", Metadata.ASCII_STRING_MARSHALLER),
                "0123456_1_0123456_2_0123456_3_0123456_4_0123456_5_0123456_6_0123456_7_0123456_8_0123456_9_0123456_0_0123456_1_0123456_2_0123456_3_0123456_4_0123456_5")
        def attributes = Attributes.newBuilder()
                .set(Grpc.TRANSPORT_ATTR_REMOTE_ADDR, new InetSocketAddress(Inet4Address.getByName("30.56.100.15"), 2448))
                .build()
        when:
        def act = new EventsBuilder.NativeCall()
                .start(metadata, attributes)
                .withChain(Chain.ETHEREUM.id)
                .onItem(BlockchainOuterClass.NativeCallItem.getDefaultInstance())
                .build()
        then:
        act.size() == 1
        with(act[0].request.remote) {
            userAgent.length() == 128
            userAgent == "0123456_1_0123456_2_0123456_3_0123456_4_0123456_5_0123456_6_0123456_7_0123456_8_0123456_9_0123456_0_0123456_1_0123456_2_0123456_"
        }
    }
}
