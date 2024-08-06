package io.emeraldpay.dshackle.startup.configure

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource

class UpstreamCreatorTest {

    @ParameterizedTest
    @MethodSource("data")
    fun `test getHash`(
        inputHash: Int?,
        obj: Any,
        answer: Short,
    ) {
        val hash = UpstreamCreator.getHash(inputHash, obj, hashes)

        assertThat(hash).isEqualTo(answer)

        println(hashes)
    }

    companion object {
        private val hashes = HashSet<Short>()

        @JvmStatic
        fun data(): List<Arguments> =
            listOf(
                Arguments.of(49, 1, 49.toShort()),
                Arguments.of(4000, 1, 4000.toShort()),
                Arguments.of(24000, 1, 24000.toShort()),
                Arguments.of(null, 49, (-49).toShort()),
                Arguments.of(null, 49, (32718).toShort()),
                Arguments.of(null, 49, (-32719).toShort()),
                Arguments.of(null, 49, (-32768).toShort()),
                Arguments.of(null, 49, (-32767).toShort()),
                Arguments.of(null, 32718, (-32718).toShort()),
                Arguments.of(null, 32718, (-50).toShort()),
            )
    }
}
