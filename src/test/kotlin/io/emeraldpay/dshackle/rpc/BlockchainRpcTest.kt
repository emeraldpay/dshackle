package io.emeraldpay.dshackle.rpc

import io.emeraldpay.api.Chain
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldNotBe

class BlockchainRpcTest :
    ShouldSpec({

        should("create metrics container") {
            val metricsEthereum = BlockchainRpc.RequestMetrics(Chain.ETHEREUM)

            metricsEthereum shouldNotBe null

            val metricsBitcoin = BlockchainRpc.RequestMetrics(Chain.BITCOIN)

            metricsBitcoin shouldNotBe null
        }
    })
