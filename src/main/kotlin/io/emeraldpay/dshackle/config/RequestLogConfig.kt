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
package io.emeraldpay.dshackle.config

/**
 * Config for logging of the request made from Dshackle to an upstream
 */
class RequestLogConfig(
    val enabled: Boolean = false,
    val includeParams: Boolean = false
) {

    var filename: String = "./request_log.jsonl"

    companion object {

        fun default(): RequestLogConfig {
            return disabled()
        }

        fun disabled(): RequestLogConfig {
            return RequestLogConfig(
                enabled = false
            )
        }
    }
}
