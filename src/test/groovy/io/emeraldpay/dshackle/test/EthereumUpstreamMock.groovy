package io.emeraldpay.dshackle.test

import io.emeraldpay.dshackle.upstream.DirectEthereumApi
import io.emeraldpay.dshackle.upstream.EthereumApi
import io.emeraldpay.dshackle.upstream.EthereumHead
import io.emeraldpay.dshackle.upstream.EthereumUpstream
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
