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
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import java.io.IOException
import java.net.InetSocketAddress
import javax.annotation.PostConstruct
import javax.annotation.PreDestroy

@Service
class HealthCheckSetup(
    @Autowired private val healthConfig: HealthConfig,
    @Autowired private val multistreamHolder: MultistreamHolder
) {

    companion object {
        private val log = LoggerFactory.getLogger(HealthCheckSetup::class.java)
    }

    lateinit var server: HttpServer

    @PostConstruct
    fun start() {
        if (!healthConfig.isEnabled()) {
            return
        }
        // use standard JVM server with a single thread blocking processing
        // health check is a rare operation, no reason to set up anything complex
        try {
            log.info("Run Health Server on ${healthConfig.host}:${healthConfig.port}${healthConfig.path}")
            server = HttpServer.create(
                InetSocketAddress(
                    healthConfig.host,
                    healthConfig.port
                ),
                0
            )
            server.createContext(healthConfig.path) { httpExchange ->
                val response = if (httpExchange.requestURI.query == "detailed") {
                    getDetailedHealth()
                } else {
                    getHealth()
                }
                val ok = response.ok
                val data = response.details.joinToString("\n")
                val code = if (ok) HttpStatus.OK else HttpStatus.SERVICE_UNAVAILABLE
                log.debug("Health check response: ${code.value()} ${code.reasonPhrase} $data")
                httpExchange.sendResponseHeaders(code.value(), data.toByteArray().size.toLong())
                httpExchange.responseBody.use { os ->
                    os.write(data.toByteArray())
                }
            }
            Thread(server::start).start()
        } catch (e: IOException) {
            log.error("Failed to start Health Server", e)
        }
    }

    fun getHealth(): Detailed {
        val errors = healthConfig.configs().mapNotNull {
            val up = multistreamHolder.getUpstream(it.blockchain)
            if (!up.isAvailable()) {
                return@mapNotNull "${it.blockchain} UNAVAILABLE"
            }
            val avail = up.getAll().count { it.isAvailable() }
            if (avail < it.minAvailable) {
                return@mapNotNull "${it.blockchain} LACKS MIN AVAILABILITY [CURRENT: $avail]"
            }
            null
        }
        return if (errors.isEmpty()) {
            Detailed(true, listOf("OK"))
        } else {
            Detailed(false, errors)
        }
    }

    fun getDetailedHealth(): Detailed {
        val chains = multistreamHolder.getAvailable()
        val allEnabled = healthConfig.configs().all { chains.contains(it.blockchain) }
        var anyUnavailable = false
        val details = chains.flatMap { chain ->
            var chainUnavailable = false
            val up = multistreamHolder.getUpstream(chain)
            val required = healthConfig.chains[chain]
            if (!up.isAvailable()) {
                if (required != null) {
                    anyUnavailable = true
                }
                listOf("${chain.name} UNAVAILABLE")
            } else {
                val ups = up.getAll()
                val checks = if (required != null) {
                    val avail = ups.count { it.isAvailable() }
                    if (avail < required.minAvailable) {
                        chainUnavailable = true
                        listOf("  LACKS MIN AVAILABILITY")
                    } else emptyList()
                } else emptyList()
                val upDetails = ups.map {
                    "  ${it.getId()} ${it.getStatus()} with lag=${it.getLag()}"
                }
                val status = if (chainUnavailable) "UNAVAILABLE" else "AVAILABLE"
                anyUnavailable = anyUnavailable || chainUnavailable
                listOf("${chain.name} $status") + upDetails + checks
            }
        }
        val detailsUnavailable = if (!allEnabled) {
            healthConfig.configs()
                .filter { !chains.contains(it.blockchain) }
                .map {
                    "${it.blockchain.name} UNAVAILABLE"
                }
        } else emptyList()
        return Detailed(
            allEnabled && !anyUnavailable,
            detailsUnavailable + details
        )
    }

    @PreDestroy
    fun shutdown() {
        if (::server.isInitialized) {
            log.info("Shutting down health Server...")
            server.stop(0)
        }
    }

    data class Detailed(
        val ok: Boolean,
        val details: List<String>
    )
}
