/**
 * Copyright (c) 2020 EmeraldPay, Inc
 * Copyright (c) 2020 ETCDEV GmbH
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

import org.slf4j.LoggerFactory

class AuthConfig {

    companion object {
        private val log = LoggerFactory.getLogger(AuthConfig::class.java)
    }

    open class ClientAuth {
        var type: String? = null
    }

    class ClientBasicAuth(
        val username: String,
        val password: String,
    ) : ClientAuth()

    class ClientTlsAuth(
        var ca: String? = null,
        var certificate: String? = null,
        var key: String? = null,
    ) : ClientAuth()

    class ClientTokenAuth(
        var publicKeyPath: String? = null,
    )

    /**
     * Example config:
     * ```
     * enabled: false
     * server:
     *   certificate: "127.0.0.1.crt"
     *   key: "127.0.0.1.p8.key"
     * client:
     *   require: false
     *   ca: "ca.dshackle.test.crt"
     * ```
     */
    open class ServerTlsAuth {
        var enabled: Boolean? = null
        var certificate: String? = null
        var key: String? = null
        var clientRequire: Boolean? = null
        var clientCAs: MutableList<String> = mutableListOf()
    }
}
