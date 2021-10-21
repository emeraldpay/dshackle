/**
 * Copyright (c) 2021 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.monitoring

import com.sun.net.httpserver.HttpServer
import io.emeraldpay.dshackle.config.HealthConfig
import io.emeraldpay.dshackle.upstream.MultistreamHolder
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import java.io.IOException
import java.net.InetSocketAddress
import javax.annotation.PostConstruct

@Service
class HealthCheckSetup(
    @Autowired private val healthConfig: HealthConfig,
    @Autowired private val multistreamHolder: MultistreamHolder
) {

    companion object {
        private val log = LoggerFactory.getLogger(HealthCheckSetup::class.java)
    }

    @PostConstruct
    fun start() {
        if (!healthConfig.isEnabled()) {
            return
        }
        // use standard JVM server with a single thread blocking processing
        // health check is a rare operation, no reason to set up anything complex
        try {
            log.info("Run Health Server on ${healthConfig.host}:${healthConfig.port}${healthConfig.path}")
            val server = HttpServer.create(
                InetSocketAddress(
                    healthConfig.host,
                    healthConfig.port
                ),
                0
            )
            server.createContext(healthConfig.path) { httpExchange ->
                val response = getHealth()
                val ok = response == "OK"
                val code = if (ok) HttpStatus.OK else HttpStatus.SERVICE_UNAVAILABLE
                httpExchange.sendResponseHeaders(code.value(), response.toByteArray().size.toLong())
                httpExchange.responseBody.use { os ->
                    os.write(response.toByteArray())
                }
            }
            Thread(server::start).start()
        } catch (e: IOException) {
            log.error("Failed to start Health Server", e)
        }
    }

    fun getHealth(): String {
        val errors = healthConfig.configs().mapNotNull {
            val up = multistreamHolder.getUpstream(it.blockchain)
            if (up == null || !up.isAvailable()) {
                return@mapNotNull "${it.blockchain} UNAVAILABLE"
            }
            val avail = up.getAll().count { it.getStatus() == UpstreamAvailability.OK }
            if (avail < it.minAvailable) {
                return@mapNotNull "${it.blockchain} LACKS MIN AVAILABILITY"
            }
            null
        }
        return if (errors.isEmpty()) {
            "OK"
        } else {
            errors.joinToString("\n")
        }
    }
}