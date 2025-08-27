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
package io.emeraldpay.dshackle

import io.emeraldpay.dshackle.config.AuthConfig
import io.grpc.netty.GrpcSslContexts
import io.netty.handler.ssl.ClientAuth
import io.netty.handler.ssl.SslContext
import io.netty.handler.ssl.SslContextBuilder
import io.netty.handler.ssl.SslProvider
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

@Service
open class TlsSetup(
    @Autowired val fileResolver: FileResolver,
) {
    companion object {
        private val log = LoggerFactory.getLogger(TlsSetup::class.java)
    }

    fun setupServer(
        category: String,
        config: AuthConfig.ServerTlsAuth?,
        grpc: Boolean,
    ): SslContext? {
        if (config == null) {
            log.warn("Using insecure transport for $category")
            return null
        }
        val mustBeSecure = config.enabled != null && config.enabled!!
        val tlsDisabled = config.enabled != null && !config.enabled!!
        var hasServerCertificate = true
        if (!tlsDisabled) {
            if (StringUtils.isEmpty(config.certificate)) {
                if (mustBeSecure) {
                    log.error(
                        "tls.server.certificate property for $category is not set (path to server TLS certificate) but TLS is enabled",
                    )
                    throw IllegalArgumentException("Certificate not set")
                }
                hasServerCertificate = false
            }
            if (StringUtils.isEmpty(config.key)) {
                if (mustBeSecure) {
                    log.error("tls.server.key property for $category is not set (path to server TLS certificate key) but TLS is enabled")
                    throw IllegalArgumentException("Certificate Key not set")
                }
                hasServerCertificate = false
            }
        }
        if (mustBeSecure || (!tlsDisabled && hasServerCertificate)) {
            log.info("Using TLS for $category")
            if (SslContext.defaultServerProvider() == SslProvider.JDK) {
                log.warn("Using JDK TLS implementation. Install OpenSSL to better performance")
            }

            val sslContextBuilder =
                if (grpc) {
                    GrpcSslContexts.forServer(
                        fileResolver.resolve(config.certificate!!),
                        fileResolver.resolve(config.key!!),
                    )
                } else {
                    SslContextBuilder.forServer(
                        fileResolver.resolve(config.certificate!!),
                        fileResolver.resolve(config.key!!),
                    )
                }
            if (StringUtils.isNotEmpty(config.clientCa)) {
                log.info("Using TLS for client authentication for $category")
                sslContextBuilder.trustManager(
                    fileResolver.resolve(config.clientCa!!),
                )
                if (config.clientRequire != null && config.clientRequire!!) {
                    sslContextBuilder.clientAuth(ClientAuth.REQUIRE)
                }
            } else if (config.clientRequire != null && config.clientRequire!!) {
                throw IllegalArgumentException("Client Certificate not set")
            } else {
                log.warn("Trust all clients for $category")
            }
            return sslContextBuilder.build()
        } else {
            log.warn("Using insecure transport for $category")
        }
        return null
    }
}
