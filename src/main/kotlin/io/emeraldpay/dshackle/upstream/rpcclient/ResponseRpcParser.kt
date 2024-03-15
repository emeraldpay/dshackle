/**
 * Copyright (c) 2020 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.upstream.rpcclient

import io.emeraldpay.dshackle.upstream.ChainResponse
import org.slf4j.LoggerFactory

open class ResponseRpcParser : ResponseParser<ChainResponse>() {

    companion object {
        private val log = LoggerFactory.getLogger(ResponseRpcParser::class.java)
    }

    override fun build(state: Preparsed): ChainResponse {
        if (state.error != null) {
            return ChainResponse(null, state.error, state.id ?: ChainResponse.Id.from(-1), null)
        }
        if (state.nullResult) {
            return ChainResponse("null".toByteArray(), null, state.id ?: ChainResponse.Id.from(-1), null)
        }
        return ChainResponse(state.result, null, state.id ?: ChainResponse.Id.from(-1), null)
    }
}
