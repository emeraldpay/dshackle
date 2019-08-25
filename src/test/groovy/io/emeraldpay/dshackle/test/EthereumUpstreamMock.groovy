/**
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
package io.emeraldpay.dshackle.test

import io.emeraldpay.dshackle.upstream.ethereum.DirectEthereumApi
import io.emeraldpay.dshackle.upstream.ethereum.EthereumHead
import io.emeraldpay.dshackle.upstream.ethereum.EthereumUpstream
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.json.BlockJson
import org.jetbrains.annotations.NotNull

class EthereumUpstreamMock extends EthereumUpstream {

    EthereumHeadMock ethereumHeadMock = new EthereumHeadMock()

    EthereumUpstreamMock(@NotNull Chain chain, @NotNull DirectEthereumApi api) {
        super(chain, api)
        setLag(0)
        setStatus(UpstreamAvailability.OK)
    }

    void nextBlock(BlockJson<TransactionId> block) {
        ethereumHeadMock.nextBlock(block)
    }

    @Override
    EthereumHead createHead() {
        return ethereumHeadMock
    }

    @Override
    EthereumHead getHead() {
        return ethereumHeadMock
    }
}
