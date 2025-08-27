package io.emeraldpay.dshackle.proxy

import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.etherjar.rpc.RpcException
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.datatest.withData
import io.kotest.matchers.shouldBe
import java.util.Locale

class ReadRpcJsonTest :
    ShouldSpec({

        val reader = ReadRpcJson()

        context("Get first symbol") {
            withData(
                Pair("{", "{ }"),
                Pair("{", "   { }"),
                Pair("{", "\n\n { }"),
                Pair("[", " [ { } ] "),
            ) {
                reader.getStartOfJson(it.second.toByteArray()) shouldBe it.first.toByteArray()[0]
            }
        }

        should("Error for input with many spaces") {
            val empty = " ".repeat(1000)
            shouldThrow<IllegalArgumentException> {
                reader.getStartOfJson(("$empty{}").toByteArray())
            }
        }

        should("Error for empty spaces") {
            shouldThrow<IllegalArgumentException> {
                reader.getStartOfJson("".toByteArray())
            }
        }

        context("Get type") {
            withData(
                Pair(ProxyCall.RpcType.SINGLE, "{}"),
                Pair(ProxyCall.RpcType.SINGLE, "   { }"),
                Pair(ProxyCall.RpcType.SINGLE, "\n\n { }"),
                Pair(ProxyCall.RpcType.BATCH, " [ { } ] "),
            ) {
                reader.getType(it.second.toByteArray()) shouldBe it.first
            }
        }

        should("Error type for invalid input") {
            shouldThrow<RpcException> {
                reader.getType("hello".toByteArray())
            }
            shouldThrow<RpcException> {
                reader.getType("1".toByteArray())
            }
            shouldThrow<RpcException> {
                reader.getType("".toByteArray())
            }
        }

        should("Parse basic") {
            val act = reader.apply("""{"jsonrpc":"2.0", "method":"net_peerCount", "id":1, "params":[]}""".toByteArray())
            act.type shouldBe ProxyCall.RpcType.SINGLE
            act.ids.size shouldBe 1
            act.ids[0] shouldBe 1
            act.items.size shouldBe 1
            with(act.items[0]) {
                id shouldBe 0
                method shouldBe "net_peerCount"
                payload.toStringUtf8() shouldBe "[]"
            }
        }

        should("Parse basic with string id") {
            val act = reader.apply("""{"jsonrpc":"2.0", "method":"net_peerCount", "id":"ggk19K5a", "params":[]}""".toByteArray())
            act.type shouldBe ProxyCall.RpcType.SINGLE
            act.ids.size shouldBe 1
            act.ids[0] shouldBe "ggk19K5a"
            act.items.size shouldBe 1
            with(act.items[0]) {
                id shouldBe 0
                method shouldBe "net_peerCount"
                payload.toStringUtf8() shouldBe "[]"
            }
        }

        should("Parse basic without params") {
            val act = reader.apply("""{"jsonrpc":"2.0", "method":"net_peerCount", "id":2}""".toByteArray())
            act.type shouldBe ProxyCall.RpcType.SINGLE
            act.ids.size shouldBe 1
            act.ids[0] shouldBe 2
            act.items.size shouldBe 1
            with(act.items[0]) {
                id shouldBe 0
                method shouldBe "net_peerCount"
                payload.toStringUtf8() shouldBe "[]"
            }
        }

        should("Parse with null params") {
            val act = reader.apply("""{"jsonrpc":"2.0", "method":"net_peerCount", "id":2, "params": null}""".toByteArray())
            act.type shouldBe ProxyCall.RpcType.SINGLE
            act.ids.size shouldBe 1
            act.ids[0] shouldBe 2
            act.items.size shouldBe 1
            with(act.items[0]) {
                id shouldBe 0
                method shouldBe "net_peerCount"
                payload.toStringUtf8() shouldBe "[]"
            }
        }

        should("Parse with parameters") {
            val act = reader.apply("""{"jsonrpc":"2.0", "method":"net_peerCount", "id":1, "params":["0x015f"]}""".toByteArray())
            act.type shouldBe ProxyCall.RpcType.SINGLE
            act.ids.size shouldBe 1
            act.ids[0] shouldBe 1
            act.items.size shouldBe 1
            with(act.items[0]) {
                id shouldBe 0
                method shouldBe "net_peerCount"
                payload.toStringUtf8() shouldBe """["0x015f"]"""
            }
        }

        should("Parse single batch") {
            val act = reader.apply("""[{"jsonrpc":"2.0", "method":"net_peerCount", "id":1, "params":[]}]""".toByteArray())
            act.type shouldBe ProxyCall.RpcType.BATCH
            act.ids.size shouldBe 1
            act.ids[0] shouldBe 1
            act.items.size shouldBe 1
            with(act.items[0]) {
                id shouldBe 0
                method shouldBe "net_peerCount"
                payload.toStringUtf8() shouldBe "[]"
            }
        }

        should("Parse multi batch") {
            val act =
                reader.apply(
                    """[{"jsonrpc":"2.0", "method":"net_peerCount", "id":"xdd", "params":[]}, {"jsonrpc":"2.0", "method":"foo_bar", "id":4, "params":[143, false]}]"""
                        .toByteArray(),
                )
            act.type shouldBe ProxyCall.RpcType.BATCH
            act.ids.size shouldBe 2
            act.ids[0] shouldBe "xdd"
            act.ids[1] shouldBe 4
            act.items.size shouldBe 2
            with(act.items[0]) {
                id shouldBe 0
                method shouldBe "net_peerCount"
                payload.toStringUtf8() shouldBe "[]"
            }
            with(act.items[1]) {
                id shouldBe 1
                method shouldBe "foo_bar"
                payload.toStringUtf8() shouldBe """[143,false]"""
            }
        }

        should("Error if id is not set") {
            val t =
                shouldThrow<RpcException> {
                    reader.apply("""{"jsonrpc":"2.0", "method":"net_peerCount", "params":[]}""".toByteArray())
                }
            t.code shouldBe -32600
            t.rpcMessage.lowercase(Locale.getDefault()) shouldBe "id is not set"
        }

        should("Error if jsonrpc is not set") {
            val t =
                shouldThrow<RpcException> {
                    reader.apply("""{"id":2, "method":"net_peerCount", "params":[]}""".toByteArray())
                }
            t.code shouldBe -32600
            t.rpcMessage.lowercase(Locale.getDefault()) shouldBe "jsonrpc version is not set"
            t.details shouldBe JsonRpcResponse.NumberId(2)
        }

        should("Error if jsonrpc version is invalid") {
            val t =
                shouldThrow<RpcException> {
                    reader.apply("""{"id":2, "jsonrpc":"3.0", "method":"net_peerCount", "params":[]}""".toByteArray())
                }
            t.code shouldBe -32600
            t.rpcMessage.lowercase(Locale.getDefault()) shouldBe "unsupported json rpc version: 3.0"
            t.details shouldBe JsonRpcResponse.NumberId(2)
        }

        should("Error if method is not set") {
            val t =
                shouldThrow<RpcException> {
                    reader.apply("""{"id":2,  "jsonrpc":"2.0", "params":[]}""".toByteArray())
                }
            t.code shouldBe -32600
            t.rpcMessage.lowercase(Locale.getDefault()) shouldBe "method is not set"
            t.details shouldBe JsonRpcResponse.NumberId(2)
        }

        should("Error if params is not array") {
            val t =
                shouldThrow<RpcException> {
                    reader.apply("""{"id":2, "jsonrpc":"2.0", "method":"test", "params":123}""".toByteArray())
                }
            t.code shouldBe -32600
            t.rpcMessage.lowercase(Locale.getDefault()) shouldBe "params must be an array"
            t.details shouldBe JsonRpcResponse.NumberId(2)
        }

        should("Error if json is broken") {
            val t =
                shouldThrow<RpcException> {
                    reader.apply("""{"id":2, "method":"net_peerCount", "params" """.toByteArray())
                }
            t.code shouldBe -32700
        }
    })
