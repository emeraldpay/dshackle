/**
 * Copyright (c) 2020 EmeraldPay, Inc
 * Copyright (c) 2019 ETCDEV GmbH
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

import java.time.Duration

class Defaults {

    companion object {
        const val maxMessageSize: Int = 32 * 1024 * 1024
        const val maxMetadataSize = 16384
        val timeout: Duration = Duration.ofSeconds(60)
        val timeoutInternal: Duration = timeout.dividedBy(4)
        val internalCallsTimeout = Duration.ofSeconds(3)
        val retryConnection: Duration = Duration.ofSeconds(10)
        val grpcServerKeepAliveTime: Long = 15 // seconds
        val grpcServerKeepAliveTimeout: Long = 5
        val grpcServerPermitKeepAliveTime: Long = 15
        val grpcServerMaxConnectionIdle: Long = 3600
        val multistreamUnavailableMethodDisableDuration: Long = 20 // minutes
    }
}
