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

/**
 * Update configuration value from environment variables. Format: ${ENV_VAR_NAME}
 */
class EnvVariables {

    companion object {
        private val envRegex = Regex("\\$\\{(\\w+?)}")
    }

    fun postProcess(value: String): String {
        return envRegex.replace(value) { m ->
            m.groups[1]?.let { g ->
                System.getProperty(g.value) ?: System.getenv(g.value) ?: ""
            } ?: ""
        }
    }
}
