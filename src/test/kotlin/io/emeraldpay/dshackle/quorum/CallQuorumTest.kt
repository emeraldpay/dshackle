package io.emeraldpay.dshackle.quorum

import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcError
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcException
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import org.springframework.http.HttpStatus

class CallQuorumTest : ShouldSpec({

    should("Understand errors for unavailable connection") {
        listOf(
            HttpStatus.BAD_GATEWAY,
            HttpStatus.GATEWAY_TIMEOUT,
            HttpStatus.SERVICE_UNAVAILABLE,
            HttpStatus.UNAUTHORIZED,
            HttpStatus.TOO_MANY_REQUESTS,
        ).forEach { status ->
            CallQuorum.isConnectionUnavailable(
                JsonRpcException(JsonRpcResponse.NumberId(1), JsonRpcError(-32000, "test"), status.value())
            ) shouldBe true
        }
    }

    should("Understand errors for available connection") {
        // see https://github.com/bitcoin/bitcoin/pull/15381
        listOf(
            HttpStatus.INTERNAL_SERVER_ERROR,
            HttpStatus.NOT_FOUND,
        ).forEach { status ->
            CallQuorum.isConnectionUnavailable(
                JsonRpcException(JsonRpcResponse.NumberId(1), JsonRpcError(-32000, "test"), status.value())
            ) shouldBe false
        }
    }
})
