package io.emeraldpay.dshackle.reader

import io.emeraldpay.dshackle.quorum.BroadcastQuorum
import io.emeraldpay.dshackle.quorum.MaximumValueQuorum
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.Selector
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.kotlin.mock
import org.springframework.cloud.sleuth.Tracer

class RpcReaderFactoryTest {
    private val defaultFactory = RpcReaderFactory.Default()

    @ParameterizedTest
    @MethodSource("data")
    fun `create BroadcastReader for MaximumValueQuorum and BroadcastQuorum`(
        rpcReaderData: RpcReaderFactory.RpcReaderData,
    ) {
        val reader = defaultFactory.create(rpcReaderData)

        assertTrue(reader is BroadcastReader)
    }

    companion object {
        private val ms = mock<Multistream>()
        private val tracer = mock<Tracer>()

        @JvmStatic
        fun data(): List<Arguments> {
            return listOf(
                Arguments.of(
                    RpcReaderFactory.RpcReaderData(
                        ms,
                        "method",
                        Selector.empty,
                        MaximumValueQuorum(),
                        null,
                        tracer,
                    ),
                ),
                Arguments.of(
                    RpcReaderFactory.RpcReaderData(
                        ms,
                        "method",
                        Selector.empty,
                        BroadcastQuorum(),
                        null,
                        tracer,
                    ),
                ),
            )
        }
    }
}
