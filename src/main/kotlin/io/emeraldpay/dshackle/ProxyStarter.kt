/**
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

import io.emeraldpay.dshackle.config.MainConfig
import io.emeraldpay.dshackle.config.ProxyConfig
import io.emeraldpay.dshackle.proxy.ProxyServer
import io.emeraldpay.dshackle.proxy.ReadRpcJson
import io.emeraldpay.dshackle.proxy.WriteRpcJson
import io.emeraldpay.dshackle.rpc.NativeCall
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.env.Environment
import org.springframework.stereotype.Service
import javax.annotation.PostConstruct

/**
 * Starts HTTP proxy endpoint, if configured
 */
@Service
class ProxyStarter(
        @Autowired private val mainConfig: MainConfig,
        @Autowired private val readRpcJson: ReadRpcJson,
        @Autowired private val writeRpcJson: WriteRpcJson,
        @Autowired private val nativeCall: NativeCall,
        @Autowired private val tlsSetup: TlsSetup
) {

    companion object {
        private val log = LoggerFactory.getLogger(ProxyStarter::class.java)
    }

    @PostConstruct
    fun start() {
        val config = mainConfig.proxy
        if (config == null) {
            log.debug("Proxy server is not configured")
            return
        }
        val server = ProxyServer(config, readRpcJson, writeRpcJson, nativeCall, tlsSetup)
        server.start()
    }

}