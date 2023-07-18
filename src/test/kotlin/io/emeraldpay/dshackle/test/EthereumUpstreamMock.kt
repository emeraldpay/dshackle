package io.emeraldpay.dshackle.test

import io.emeraldpay.api.Chain
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.startup.QuorumForLabels
import io.emeraldpay.dshackle.upstream.ForkWatch
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.calls.AggregatedCallMethods
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.calls.DefaultBitcoinMethods
import io.emeraldpay.dshackle.upstream.calls.DefaultEthereumMethods
import io.emeraldpay.dshackle.upstream.calls.DirectCallMethods
import io.emeraldpay.dshackle.upstream.ethereum.EthereumRpcUpstream
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import org.reactivestreams.Publisher

class EthereumUpstreamMock(
    id: String = "test",
    chain: Chain = Chain.ETHEREUM,
    val api: Reader<JsonRpcRequest, JsonRpcResponse>,
    methods: CallMethods = allMethods(),
    val ethereumHeadMock: EthereumHeadMock = EthereumHeadMock()
) : EthereumRpcUpstream(
    id = id,
    chain = chain,
    forkWatch = ForkWatch.Never(),
    directReader = api,
    wsPool = null,
    options = UpstreamsConfig.PartialOptions.getDefaults().build(),
    role = UpstreamsConfig.UpstreamRole.PRIMARY,
    targets = methods,
    node = QuorumForLabels.QuorumItem(1, UpstreamsConfig.Labels())
) {

    companion object {
        fun allMethods(): CallMethods {
            return AggregatedCallMethods(
                listOf(
                    DefaultEthereumMethods(Chain.ETHEREUM),
                    DefaultBitcoinMethods(),
                    DirectCallMethods(listOf("eth_test"))
                )
            )
        }
    }

    var mockStatus: UpstreamAvailability? = null

    init {
        setLag(0)
        setStatus(UpstreamAvailability.OK)
        start()
    }

    fun nextBlock(block: BlockContainer) {
        ethereumHeadMock.nextBlock(block)
    }

    fun setBlocks(blocks: Publisher<BlockContainer>) {
        ethereumHeadMock.predefined = blocks
    }

    override fun getStatus(): UpstreamAvailability {
        return mockStatus ?: super.getStatus()
    }

    override fun createHead(): EthereumHeadMock {
        return ethereumHeadMock
    }

    override fun getHead(): EthereumHeadMock {
        return ethereumHeadMock
    }

    override fun toString(): String {
        return "Upstream mock ${getId()}"
    }
}
