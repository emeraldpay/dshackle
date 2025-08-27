/**
 * Copyright (c) 2022 EmeraldPay, Inc
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

import io.emeraldpay.dshackle.monitoring.accesslog.AccessContext
import io.emeraldpay.dshackle.monitoring.requestlog.RequestContext
import org.slf4j.LoggerFactory

/**
 * Global access to monitoring functions
 */
class MonitoringContext {
    companion object {
        private val log = LoggerFactory.getLogger(MonitoringContext::class.java)
    }

    /**
     * Monitoring for request made by Dshackle to upstreams
     */
    val ingress = RequestContext()

    /**
     * Monitoring for request made to Dshackle from a client
     */
    val egress = AccessContext()
}
