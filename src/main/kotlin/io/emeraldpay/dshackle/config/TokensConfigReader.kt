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

import io.emeraldpay.dshackle.Global
import org.slf4j.LoggerFactory
import org.yaml.snakeyaml.nodes.MappingNode
import java.util.Locale

class TokensConfigReader : YamlConfigReader<TokensConfig>() {

    private val log = LoggerFactory.getLogger(TokensConfigReader::class.java)

    override fun read(input: MappingNode?): TokensConfig? {
        val tokens = getList<MappingNode>(input, "tokens")?.value?.map { node ->
            val token = TokensConfig.Token()
            token.id = getValueAsString(node, "id")
            token.blockchain = getValueAsString(node, "blockchain")?.let {
                Global.chainById(it)
            }
            token.address = getValueAsString(node, "address")
            token.name = getValueAsString(node, "name")
            token.type = getValueAsString(node, "type")?.let {
                if (it.uppercase(Locale.getDefault()) == "ERC-20") {
                    TokensConfig.Type.ERC20
                } else {
                    log.warn("Invalid token type: $it")
                    null
                }
            }
            token
        }?.filter { token ->
            val invalidField = token.validate()
            if (invalidField != null) {
                log.error("Failed to parse token ${token.id}. Invalid field: $invalidField")
                false
            } else {
                true
            }
        }
        return tokens?.let {
            TokensConfig(it)
        }
    }
}
