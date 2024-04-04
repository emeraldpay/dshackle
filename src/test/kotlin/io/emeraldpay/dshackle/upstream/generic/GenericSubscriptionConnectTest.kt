package io.emeraldpay.dshackle.upstream.generic

import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.ethereum.WsSubscriptions
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import org.junit.jupiter.api.Test
import org.mockito.Mockito.verify
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import reactor.core.publisher.Flux
import reactor.test.StepVerifier
import java.time.Duration
import java.util.concurrent.atomic.AtomicReference

class GenericSubscriptionConnectTest {

    @Test
    fun `test request param is flat list`() {
        val param: List<Any> = listOf("all")
        val topic = "topic"
        val response = "hello".toByteArray()
        val ws = mock<WsSubscriptions> {
            on { subscribe(ChainRequest(topic, ListParams(param))) } doReturn
                WsSubscriptions.SubscribeData(Flux.just(response), "", AtomicReference(""))
        }

        val genericSubscriptionConnect = GenericSubscriptionConnect(ws, topic, param)

        StepVerifier.create(genericSubscriptionConnect.createConnection())
            .expectNext(response)
            .expectComplete()
            .verify(Duration.ofSeconds(1))

        verify(ws).subscribe(ChainRequest(topic, ListParams(param)))
    }
}
