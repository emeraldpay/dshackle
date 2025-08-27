package io.emeraldpay.dshackle.quorum

import io.emeraldpay.api.Chain
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.reader.QuorumRpcReader
import io.emeraldpay.dshackle.upstream.FilteredApis
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcException
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.etherjar.rpc.RpcException
import io.emeraldpay.etherjar.rpc.RpcResponseError
import io.kotest.assertions.throwables.shouldThrowAny
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.instanceOf
import io.mockk.every
import io.mockk.mockk
import reactor.core.Exceptions
import reactor.core.publisher.Mono
import java.time.Duration

class QuorumRpcReaderTest :
    ShouldSpec({

        context("always quorum") {
            should("get the result if ok") {
                val upstream = mockk<Upstream>()
                every { upstream.isAvailable() } returns true
                every { upstream.getRole() } returns UpstreamsConfig.UpstreamRole.PRIMARY
                every { upstream.getIngressReader() } returns
                    mockk {
                        every { read(JsonRpcRequest("eth_test", emptyList())) } returns Mono.just(JsonRpcResponse.ok("1"))
                    }

                val apis =
                    FilteredApis(
                        Chain.ETHEREUM,
                        listOf(upstream),
                        Selector.empty,
                    )
                val reader = QuorumRpcReader(apis, AlwaysQuorum())

                val act =
                    reader
                        .read(JsonRpcRequest("eth_test", emptyList()))
                        .map { String(it.value) }
                        .block(Duration.ofSeconds(1))

                act shouldBe "1"
            }

            should("return upstream error") {
                val upstream = mockk<Upstream>()
                every { upstream.isAvailable() } returns true
                every { upstream.getRole() } returns UpstreamsConfig.UpstreamRole.PRIMARY
                every { upstream.getIngressReader() } returns
                    mockk {
                        every { read(JsonRpcRequest("eth_test", emptyList())) } returns Mono.just(JsonRpcResponse.error(1, "test"))
                    }

                val apis =
                    FilteredApis(
                        Chain.ETHEREUM,
                        listOf(upstream),
                        Selector.empty,
                    )
                val reader = QuorumRpcReader(apis, AlwaysQuorum())

                val t =
                    shouldThrowAny {
                        reader
                            .read(JsonRpcRequest("eth_test", emptyList()))
                            .map { String(it.value) }
                            .block(Duration.ofSeconds(1))
                    }.let(Exceptions::unwrap)

                t shouldBe instanceOf<JsonRpcException>()
                (t as JsonRpcException).error.message shouldBe "test"
            }

            should("return upstream error thrown by reader") {
                val upstream = mockk<Upstream>()
                every { upstream.isAvailable() } returns true
                every { upstream.getRole() } returns UpstreamsConfig.UpstreamRole.PRIMARY
                every { upstream.getIngressReader() } returns
                    mockk {
                        every { read(JsonRpcRequest("eth_test", emptyList())) } returns
                            Mono.error(RpcException(RpcResponseError.CODE_UPSTREAM_CONNECTION_ERROR, "test"))
                    }

                val apis =
                    FilteredApis(
                        Chain.ETHEREUM,
                        listOf(upstream),
                        Selector.empty,
                    )
                val reader = QuorumRpcReader(apis, AlwaysQuorum())

                val t =
                    shouldThrowAny {
                        reader
                            .read(JsonRpcRequest("eth_test", emptyList()))
                            .map { String(it.value) }
                            .block(Duration.ofSeconds(1))
                    }.let(Exceptions::unwrap)

                t shouldBe instanceOf<JsonRpcException>()
                (t as JsonRpcException).error.message shouldBe "test"
            }
        }

        context("non empty quorum") {
            should("get the second result if first is null") {
                val upstream = mockk<Upstream>()
                every { upstream.isAvailable() } returns true
                every { upstream.getRole() } returns UpstreamsConfig.UpstreamRole.PRIMARY
                every { upstream.getIngressReader() } returns
                    mockk {
                        every { read(JsonRpcRequest("eth_test", emptyList())) } returnsMany
                            listOf(
                                Mono.just(JsonRpcResponse.ok("null")),
                                Mono.just(JsonRpcResponse.ok("1")),
                            )
                    }

                val apis =
                    FilteredApis(
                        Chain.ETHEREUM,
                        listOf(upstream),
                        Selector.empty,
                    )
                val reader = QuorumRpcReader(apis, NonEmptyQuorum(3))

                val act =
                    reader
                        .read(JsonRpcRequest("eth_test", emptyList()))
                        .map { String(it.value) }
                        .block(Duration.ofSeconds(1))

                act shouldBe "1"
            }

            should("get the second result if first is error") {
                val upstream = mockk<Upstream>()
                every { upstream.isAvailable() } returns true
                every { upstream.getRole() } returns UpstreamsConfig.UpstreamRole.PRIMARY
                every { upstream.getIngressReader() } returns
                    mockk {
                        every { read(JsonRpcRequest("eth_test", emptyList())) } returnsMany
                            listOf(
                                Mono.just(JsonRpcResponse.error(1, "test")),
                                Mono.just(JsonRpcResponse.ok("1")),
                            )
                    }

                val apis =
                    FilteredApis(
                        Chain.ETHEREUM,
                        listOf(upstream),
                        Selector.empty,
                    )
                val reader = QuorumRpcReader(apis, NonEmptyQuorum(3))

                val act =
                    reader
                        .read(JsonRpcRequest("eth_test", emptyList()))
                        .map { String(it.value) }
                        .block(Duration.ofSeconds(1))

                act shouldBe "1"
            }

            should("get the third if first two are not ok") {
                val upstream = mockk<Upstream>()
                every { upstream.isAvailable() } returns true
                every { upstream.getRole() } returns UpstreamsConfig.UpstreamRole.PRIMARY
                every { upstream.getIngressReader() } returns
                    mockk {
                        every { read(JsonRpcRequest("eth_test", emptyList())) } returnsMany
                            listOf(
                                Mono.just(JsonRpcResponse.ok("null")),
                                Mono.just(JsonRpcResponse.error(1, "test")),
                                Mono.just(JsonRpcResponse.ok("1")),
                            )
                    }

                val apis =
                    FilteredApis(
                        Chain.ETHEREUM,
                        listOf(upstream),
                        Selector.empty,
                    )
                val reader = QuorumRpcReader(apis, NonEmptyQuorum(3))

                val act =
                    reader
                        .read(JsonRpcRequest("eth_test", emptyList()))
                        .map { String(it.value) }
                        .block(Duration.ofSeconds(1))

                act shouldBe "1"
            }

            should("error if all failed") {
                val upstream = mockk<Upstream>()
                every { upstream.isAvailable() } returns true
                every { upstream.getRole() } returns UpstreamsConfig.UpstreamRole.PRIMARY
                every { upstream.getIngressReader() } returns
                    mockk {
                        every { read(JsonRpcRequest("eth_test", emptyList())) } returnsMany
                            listOf(
                                Mono.just(JsonRpcResponse.error(1, "test")),
                                Mono.just(JsonRpcResponse.error(1, "test")),
                                Mono.just(JsonRpcResponse.error(1, "test")),
                            )
                    }

                val apis =
                    FilteredApis(
                        Chain.ETHEREUM,
                        listOf(upstream),
                        Selector.empty,
                    )
                val reader = QuorumRpcReader(apis, NonEmptyQuorum(3))

                val t =
                    shouldThrowAny {
                        reader
                            .read(JsonRpcRequest("eth_test", emptyList()))
                            .map { String(it.value) }
                            .block(Duration.ofSeconds(1))
                    }.let(Exceptions::unwrap)

                t shouldBe instanceOf<JsonRpcException>()
                (t as JsonRpcException).error.message shouldBe "test"
            }
        }

        context("not lagging quorum") {

            should("return error if upstream returned it") {
                val upstream = mockk<Upstream>()
                every { upstream.isAvailable() } returns true
                every { upstream.getLag() } returns 0
                every { upstream.getRole() } returns UpstreamsConfig.UpstreamRole.PRIMARY
                every { upstream.getIngressReader() } returns
                    mockk {
                        every { read(JsonRpcRequest("eth_test", emptyList())) } returns Mono.just(JsonRpcResponse.error(-3010, "test"))
                    }

                val apis =
                    FilteredApis(
                        Chain.ETHEREUM,
                        listOf(upstream),
                        Selector.empty,
                    )
                val reader = QuorumRpcReader(apis, NotLaggingQuorum(1))

                val t =
                    shouldThrowAny {
                        reader
                            .read(JsonRpcRequest("eth_test", emptyList()))
                            .map { String(it.value) }
                            .block(Duration.ofSeconds(1))
                    }.let(Exceptions::unwrap)

                t shouldBe instanceOf<JsonRpcException>()
                (t as JsonRpcException).error.message shouldBe "test"
                (t as JsonRpcException).error.code shouldBe -3010
            }
        }
    })
