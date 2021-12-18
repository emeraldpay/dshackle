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
package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.config.AuthConfig
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.dshackle.upstream.rpcclient.RpcMetrics
import java.net.URI

class EthereumWsFactory(
    private val uri: URI,
    private val origin: URI
) {

    var basicAuth: AuthConfig.ClientBasicAuth? = null
    var config: UpstreamsConfig.WsEndpoint? = null

    fun create(upstream: DefaultUpstream?, validator: EthereumUpstreamValidator?, rpcMetrics: RpcMetrics?): WsConnection {
        return WsConnection(uri, origin, basicAuth, rpcMetrics, upstream, validator).also { ws ->
            config?.frameSize?.let {
                ws.frameSize = it
            }
            config?.msgSize?.let {
                ws.msgSizeLimit = it
            }
        }
    }
}
