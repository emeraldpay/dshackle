package io.emeraldpay.dshackle.reader

import io.emeraldpay.dshackle.quorum.AlwaysQuorum
import io.emeraldpay.dshackle.quorum.NotLaggingQuorum
import io.emeraldpay.dshackle.upstream.Capability
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleRequest
import io.emeraldpay.dshackle.upstream.signature.NoSigner
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.instanceOf
import io.mockk.every
import io.mockk.mockk

class MultistreamReaderTest :
    ShouldSpec({

        should("always match an RPC upstream") {
            val reader = MultistreamReader(mockk(), NoSigner())

            val matcher =
                reader.createMatcher(
                    DshackleRequest("eth_test", emptyList()),
                    AlwaysQuorum(),
                )

            matcher shouldBe instanceOf<Selector.MultiMatcher>()

            val act = (matcher as Selector.MultiMatcher).getMatchers().filterIsInstance<Selector.CapabilityMatcher>()
            act shouldHaveSize 1
            act.first().capability shouldBe Capability.RPC
        }

        should("always match method name") {
            val reader = MultistreamReader(mockk(), NoSigner())

            val matcher =
                reader.createMatcher(
                    DshackleRequest("eth_test", emptyList()),
                    AlwaysQuorum(),
                )

            matcher shouldBe instanceOf<Selector.MultiMatcher>()

            val act = (matcher as Selector.MultiMatcher).getMatchers().filterIsInstance<Selector.MethodMatcher>()
            act shouldHaveSize 1
            act.first().method shouldBe "eth_test"
        }

        should("should match height for NotLaggingQuorum") {
            val up = mockk<Multistream>()
            every { up.getHead() } returns
                mockk<Head> {
                    every { getCurrentHeight() } returns 123L
                }

            val reader = MultistreamReader(up, NoSigner())

            val matcher =
                reader.createMatcher(
                    DshackleRequest("eth_test", emptyList()),
                    NotLaggingQuorum(1),
                )

            matcher shouldBe instanceOf<Selector.MultiMatcher>()

            val act = (matcher as Selector.MultiMatcher).getMatchers().filterIsInstance<Selector.HeightMatcher>()
            act shouldHaveSize 1
            act.first().height shouldBe 122
        }

        should("should not match height for AlwaysQuorum") {
            val up = mockk<Multistream>()
            every { up.getHead() } returns
                mockk<Head> {
                    every { getCurrentHeight() } returns 123L
                }

            val reader = MultistreamReader(up, NoSigner())

            val matcher =
                reader.createMatcher(
                    DshackleRequest("eth_test", emptyList()),
                    AlwaysQuorum(),
                )

            matcher shouldBe instanceOf<Selector.MultiMatcher>()

            val act = (matcher as Selector.MultiMatcher).getMatchers().any { it is Selector.HeightMatcher }
            act shouldBe false
        }
    })
