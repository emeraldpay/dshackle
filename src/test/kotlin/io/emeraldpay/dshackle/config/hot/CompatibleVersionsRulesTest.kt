package io.emeraldpay.dshackle.config.hot

import io.emeraldpay.dshackle.Global
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class CompatibleVersionsRulesTest {
    @Test
    fun `test parsing`() {
        val raw = """
            rules:
              - client: "client1"
                blacklist: 
                    - 1.0.0
                    - 1.0.1
                whitelist: 
                    - 1.0.2
                    - 1.0.3
              - client: "client2"
                blacklist: 
                    - 1.0.0
                    - 1.0.1
                whitelist: 
                    - 1.0.2
                    - 1.0.3
        """.trimIndent()

        val rules = Global.yamlMapper.readValue(raw, CompatibleVersionsRules::class.java)!!.rules
        assertEquals(2, rules.size)
        assertEquals("client1", rules[0].client)
        assertEquals(2, rules[0].blacklist!!.size)
        assertEquals("1.0.0", rules[0].blacklist!![0])
        assertEquals("1.0.1", rules[0].blacklist!![1])
        assertEquals(2, rules[0].whitelist!!.size)
        assertEquals("1.0.2", rules[0].whitelist!![0])
        assertEquals("1.0.3", rules[0].whitelist!![1])
        assertEquals("client2", rules[1].client)
        assertEquals(2, rules[1].blacklist!!.size)
        assertEquals("1.0.0", rules[1].blacklist!![0])
        assertEquals("1.0.1", rules[1].blacklist!![1])
        assertEquals(2, rules[1].whitelist!!.size)
        assertEquals("1.0.2", rules[1].whitelist!![0])
        assertEquals("1.0.3", rules[1].whitelist!![1])
    }
}
