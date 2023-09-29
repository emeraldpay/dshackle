package io.emeraldpay.dshackle.foundation

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.nodes.MappingNode
import java.io.StringReader

class YamlConfigReaderTest {

    @ParameterizedTest
    @CsvSource(
        "1024, 1024",
        "1k, 1024",
        "1kb, 1024",
        "1K, 1024",
        "16kb, 16384",
        "1M, 1048576",
        "4mb, 4194304",
    )
    fun `reads bytes values`(input: String, expected: Int) {
        val rdr = Impl()
        assertEquals(expected, rdr.getValueAsBytes(rdr.asNode("test", input), "test"))
    }

    @Test
    fun `mergeMappingNode should merge nodes correctly`() {
        val yaml = Yaml()

        val a = yaml.compose(
            StringReader(
                """
            key1: value1
            key2:
              subkey1: subvalue1
        """,
            ),
        ) as MappingNode

        val b = yaml.compose(
            StringReader(
                """
            key1: value3
            key2:
              subkey1: subvalue1Modified
              subkey2: subvalue2
            key3: value3
        """,
            ),
        ) as MappingNode

        val result = Impl().mergeMappingNode(a, b)!!
        val yml = Impl()

        assertEquals(3, result.value.size)
        assertEquals("value3", yml.getNodeValue(result, "key1"))
        assertEquals("subvalue1Modified", yml.getNodeValue(yml.getNode(result, "key2") as MappingNode, "subkey1"))
        assertEquals("subvalue2", yml.getNodeValue(yml.getNode(result, "key2") as MappingNode, "subkey2"))
        assertEquals("value3", yml.getNodeValue(result, "key3"))
    }

    class Impl : YamlConfigReader<Any>() {
        override fun read(input: MappingNode?): Any? {
            return null
        }
        fun getNode(node: MappingNode, key: String): Any? {
            return Impl().getMapping(node, key)
        }

        fun getNodeValue(node: MappingNode, key: String): String? {
            return Impl().getValueAsString(node, key)
        }

        fun asNode(key: String, value: String): MappingNode {
            return Yaml().compose(StringReader("$key: $value")) as MappingNode
        }
    }
}
