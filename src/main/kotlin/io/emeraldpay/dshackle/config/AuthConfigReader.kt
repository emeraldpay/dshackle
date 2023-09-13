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
import org.yaml.snakeyaml.nodes.MappingNode
import org.yaml.snakeyaml.nodes.ScalarNode

class AuthConfigReader : YamlConfigReader<AuthConfig>() {

    companion object {
        private val log = LoggerFactory.getLogger(AuthConfigReader::class.java)
    }

    fun readClientBasicAuth(node: MappingNode?): AuthConfig.ClientBasicAuth? {
        return getMapping(node, "basic-auth")?.let { authNode ->
            val username = getValueAsString(authNode, "username")
            val password = getValueAsString(authNode, "password")
            if (username != null && password != null) {
                AuthConfig.ClientBasicAuth(username, password)
            } else {
                log.warn("Basic auth is not fully configured")
                null
            }
        }
    }

    fun readClientTls(node: MappingNode?): AuthConfig.ClientTlsAuth? {
        return getMapping(node, "tls")?.let { authNode ->
            val auth = AuthConfig.ClientTlsAuth()
            auth.ca = getValueAsString(authNode, "ca")
            auth.certificate = getValueAsString(authNode, "certificate")
            auth.key = getValueAsString(authNode, "key")
            auth
        }
    }

    fun readTokenAuth(node: MappingNode?): AuthConfig.ClientTokenAuth? {
        return getMapping(node, "token-auth")?.let {
            val auth = AuthConfig.ClientTokenAuth()
            auth.publicKeyPath = getValueAsString(it, "public-key")
            auth
        }
    }

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
    fun readServerTls(rootNode: MappingNode?): AuthConfig.ServerTlsAuth? {
        return getMapping(rootNode, "tls")?.let { tlsNode ->
            val auth = AuthConfig.ServerTlsAuth()
            getValueAsBool(tlsNode, "enabled")?.let {
                auth.enabled = it
            }
            if (auth.enabled != null && !auth.enabled!!) {
                return null
            }
            getMapping(tlsNode, "server")?.let { serverNode ->
                auth.certificate = getValueAsString(serverNode, "certificate")
                auth.key = getValueAsString(serverNode, "key")
            }
            getMapping(tlsNode, "client")?.let { clientNode ->
                getValueAsBool(clientNode, "require")?.let {
                    auth.clientRequire = it
                }
                getValueAsString(clientNode, "ca")?.let {
                    auth.clientCAs.add(it)
                }
                getList<ScalarNode>(clientNode, "cas")?.let {
                    println(it)
                    it.value?.let { crt ->
                        auth.clientCAs.addAll(crt.map { v -> v.value })
                    }
                }
            }
            auth
        }
    }

    override fun read(input: MappingNode?): AuthConfig? {
        return null
    }
}
