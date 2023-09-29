package io.emeraldpay.dshackle.rpc

import io.emeraldpay.api.proto.BlockchainOuterClass.AndSelector
import io.emeraldpay.api.proto.BlockchainOuterClass.LabelSelector
import io.emeraldpay.api.proto.BlockchainOuterClass.NotSelector
import io.emeraldpay.api.proto.BlockchainOuterClass.OrSelector
import io.emeraldpay.api.proto.BlockchainOuterClass.Selector
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.stream.Stream

internal class SelectorsTest {

    companion object {
        @JvmStatic
        fun data(): Stream<Arguments> {
            val leafLabelSelector = LabelSelector.newBuilder().build()

            return Stream.of(
                Arguments.of(
                    Selector.newBuilder().setLabelSelector(leafLabelSelector).build(),
                    null,
                ),
                Arguments.of(
                    Selector.newBuilder().setShouldBeForwarded(true)
                        .setLabelSelector(leafLabelSelector).build(),
                    Selector.newBuilder().setShouldBeForwarded(true)
                        .setLabelSelector(leafLabelSelector).build(),
                ),
                Arguments.of(
                    Selector.newBuilder().setOrSelector(OrSelector.newBuilder()).build(),
                    null,
                ),
                Arguments.of(
                    Selector.newBuilder().setShouldBeForwarded(true)
                        .setOrSelector(OrSelector.newBuilder()).build(),
                    null,
                ),
                Arguments.of(
                    Selector.newBuilder().setShouldBeForwarded(true)
                        .setOrSelector(
                            OrSelector.newBuilder().addSelectors(
                                Selector.newBuilder().setLabelSelector(leafLabelSelector),
                            ),
                        ).build(),
                    null,
                ),
                Arguments.of(
                    Selector.newBuilder().setShouldBeForwarded(true)
                        .setOrSelector(
                            OrSelector.newBuilder().addSelectors(
                                Selector.newBuilder().setShouldBeForwarded(true)
                                    .setLabelSelector(leafLabelSelector),
                            ),
                        ).build(),
                    Selector.newBuilder().setShouldBeForwarded(true)
                        .setLabelSelector(leafLabelSelector).build(),
                ),
                Arguments.of(
                    Selector.newBuilder().setShouldBeForwarded(true)
                        .setOrSelector(
                            OrSelector.newBuilder()
                                .addSelectors(Selector.newBuilder().setShouldBeForwarded(true).setLabelSelector(leafLabelSelector))
                                .addSelectors(Selector.newBuilder().setShouldBeForwarded(true).setLabelSelector(leafLabelSelector))
                                .addSelectors(Selector.newBuilder().setLabelSelector(leafLabelSelector)),
                        ).build(),
                    Selector.newBuilder().setShouldBeForwarded(true)
                        .setOrSelector(
                            OrSelector.newBuilder()
                                .addSelectors(Selector.newBuilder().setShouldBeForwarded(true).setLabelSelector(leafLabelSelector))
                                .addSelectors(Selector.newBuilder().setShouldBeForwarded(true).setLabelSelector(leafLabelSelector)),
                        ).build(),
                ),
                Arguments.of(
                    Selector.newBuilder().setShouldBeForwarded(true)
                        .setAndSelector(
                            AndSelector.newBuilder()
                                .addSelectors(Selector.newBuilder().setShouldBeForwarded(true).setLabelSelector(leafLabelSelector))
                                .addSelectors(Selector.newBuilder().setShouldBeForwarded(true).setLabelSelector(leafLabelSelector))
                                .addSelectors(Selector.newBuilder().setLabelSelector(leafLabelSelector)),
                        ).build(),
                    Selector.newBuilder().setShouldBeForwarded(true)
                        .setAndSelector(
                            AndSelector.newBuilder()
                                .addSelectors(Selector.newBuilder().setShouldBeForwarded(true).setLabelSelector(leafLabelSelector))
                                .addSelectors(Selector.newBuilder().setShouldBeForwarded(true).setLabelSelector(leafLabelSelector)),
                        ).build(),
                ),
                Arguments.of(
                    Selector.newBuilder().setShouldBeForwarded(true)
                        .setNotSelector(
                            NotSelector.newBuilder()
                                .setSelector(Selector.newBuilder().setShouldBeForwarded(true).setLabelSelector(leafLabelSelector)),
                        ).build(),
                    Selector.newBuilder().setShouldBeForwarded(true)
                        .setNotSelector(
                            NotSelector.newBuilder()
                                .setSelector(Selector.newBuilder().setShouldBeForwarded(true).setLabelSelector(leafLabelSelector)),
                        ).build(),
                ),
                Arguments.of(
                    Selector.newBuilder().setShouldBeForwarded(true)
                        .setNotSelector(
                            NotSelector.newBuilder()
                                .setSelector(Selector.newBuilder().setLabelSelector(leafLabelSelector)),
                        ).build(),
                    null,
                ),

            )
        }
    }

    @ParameterizedTest
    @MethodSource("data")
    fun testKeepForwarded(input: Selector, expected: Selector?) {
        assertEquals(expected, Selectors.keepForwarded(input))
    }
}
