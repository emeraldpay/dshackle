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
package io.emeraldpay.dshackle.proxy

import io.emeraldpay.api.proto.BlockchainOuterClass
import org.slf4j.LoggerFactory

/**
 * JSON RPC call to the proxy
 */
class ProxyCall(
    /**
     * Type of the request. The response format depends on it
     */
    val type: RpcType,
) {
    companion object {
        private val log = LoggerFactory.getLogger(ProxyCall::class.java)
    }

    /**
     * Mapping from our internal ids to user provided JSON RPC ids.
     */
    val ids = ArrayList<Any>()

    /**
     * Content of the request
     */
    val items: MutableList<BlockchainOuterClass.NativeCallItem> = ArrayList()

    enum class RpcType {
        /**
         * One item request passed as Object
         */
        SINGLE,

        /**
         * Batch passed as Array of Object. It may be one-element array, i.e., single request, though response
         * must be formatted as an Array
         */
        BATCH,
    }
}
