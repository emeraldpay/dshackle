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
package io.emeraldpay.dshackle.upstream

import reactor.core.publisher.Mono

/**
 * A general interface to make a request to an Upstream API
 */
interface UpstreamApi {

    /**
     * @param id an internal uniq id, if multiple requests are made in batch
     * @param method JSON RPC method name
     * @param params JSON RPC parameters, must be serializable into a JSON array
     */
    fun execute(id: Int, method: String, params: List<Any>): Mono<ByteArray>

}