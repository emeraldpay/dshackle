/**
 * Copyright (c) 2020 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.config

import io.emeraldpay.dshackle.foundation.EnvVariables
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.ValueSource

class EnvVariablesTest {

    lateinit var reader: EnvVariables

    @BeforeEach
    fun setup() {
        reader = EnvVariables()
    }

    @ParameterizedTest
    @ValueSource(strings = ["", "a", "13143", "/etc/client1.myservice.com.key", "true", "1a68f20154fc258fe4149c199ad8f281"])
    fun `Post process for usual strings`(s: String) {
        assertEquals(s, reader.postProcess(s))
    }

    @ParameterizedTest
    @CsvSource(
        "p_\${id}, p_1",
        "home: \${HOME}, home: /home/user",
        "\${PASSWORD}, 1a68f20154fc258fe4149c199ad8f281",
    )
    fun `Post process replaces from env`(orig: String, replaced: String) {
        System.setProperty("id", "1")
        System.setProperty("HOME", "/home/user")
        System.setProperty("PASSWORD", "1a68f20154fc258fe4149c199ad8f281")
        assertEquals(replaced, reader.postProcess(orig))
    }
}
