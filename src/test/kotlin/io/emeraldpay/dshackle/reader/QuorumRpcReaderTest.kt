package io.emeraldpay.dshackle.reader

import io.emeraldpay.dshackle.quorum.AlwaysQuorum
import io.emeraldpay.dshackle.upstream.ApiSource
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcError
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcException
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.kotest.core.spec.style.ShouldSpec
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify

class QuorumRpcReaderTest :
    ShouldSpec({

        should("Exclude upstream from immediate retries on connection error") {
            val quorum = AlwaysQuorum()
            val upstream = mockk<Upstream>()
            val apiSource =
                mockk<ApiSource> {
                    every { request(any()) } returns Unit
                    every { exclude(upstream) } returns Unit
                }

            val reader = QuorumRpcReader(apiSource, quorum)

            reader.handleError<String>(
                JsonRpcException(JsonRpcResponse.NumberId(1), JsonRpcError(-32000, "test"), 429),
                JsonRpcRequest("test", emptyList()),
                upstream,
            )

            verify { apiSource.exclude(upstream) }
        }
    })
