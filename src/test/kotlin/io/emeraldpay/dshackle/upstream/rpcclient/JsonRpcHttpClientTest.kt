package io.emeraldpay.dshackle.upstream.rpcclient

import io.emeraldpay.dshackle.config.AuthConfig
import io.emeraldpay.dshackle.test.NoOpChannelMetricsRecorder
import io.emeraldpay.dshackle.test.TestingCommonsKotlin
import io.emeraldpay.etherjar.rpc.RpcResponseError
import io.kotest.assertions.throwables.shouldThrowAny
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.instanceOf
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.Timer
import org.mockserver.integration.ClientAndServer
import org.mockserver.model.HttpRequest
import org.mockserver.model.HttpResponse
import org.mockserver.model.MediaType
import org.springframework.util.SocketUtils
import reactor.core.Exceptions
import java.time.Duration

class JsonRpcHttpClientTest : ShouldSpec({

    val port = SocketUtils.findAvailableTcpPort(20000)
    val metrics = RpcMetrics(
        Timer.builder("test1").register(TestingCommonsKotlin.meterRegistry),
        Counter.builder("test2").register(TestingCommonsKotlin.meterRegistry),
        DistributionSummary.builder("test3").register(TestingCommonsKotlin.meterRegistry),
        NoOpChannelMetricsRecorder.Instance
    )

    var mockServer: ClientAndServer? = null

    beforeEach {
        mockServer = ClientAndServer.startClientAndServer(port)
    }

    afterEach {
        mockServer?.stop()
    }

    should("Make a request") {
        val client = JsonRpcHttpClient("localhost:$port", metrics, null, null)

        val resp = """
        {
            "jsonrpc": "2.0",
            "result": "0x98de45",
            "error": null,
            "id": 15
        }
        """.trimIndent()

        mockServer!!.`when`(HttpRequest.request())
            .respond(HttpResponse.response(resp))

        val act = client.read(JsonRpcRequest("test", emptyList())).block(Duration.ofSeconds(1))

        act.error shouldBe null
        act.resultAsProcessedString shouldBe "0x98de45"
    }

    should("Mare request with basic auth") {
        val auth = AuthConfig.ClientBasicAuth("user", "passwd")
        val client = JsonRpcHttpClient("localhost:$port", metrics, auth, null)

        mockServer!!.`when`(HttpRequest.request().withMethod("POST").withBody("ping"))
            .respond(HttpResponse.response().withBody("pong"))

        val act = client.execute("ping".toByteArray()).map { String(it.t2) }.block(Duration.ofSeconds(1))

        act shouldBe "pong"

        mockServer!!.verify(
            HttpRequest.request()
                .withMethod("POST")
                .withBody("ping")
                .withContentType(MediaType.APPLICATION_JSON)
                .withHeader("authorization", "Basic dXNlcjpwYXNzd2Q=")
        )
    }

    should("Produce RPC Exception on 500 status code") {
        val client = JsonRpcHttpClient("localhost:$port", metrics, null, null)

        mockServer!!.`when`(HttpRequest.request())
            .respond(HttpResponse.response().withStatusCode(500).withBody("pong"))

        val t = shouldThrowAny {
            client.read(JsonRpcRequest("test", emptyList())).block(Duration.ofSeconds(1))
        }.let(Exceptions::unwrap)

        t shouldBe instanceOf<JsonRpcException>()
        (t as JsonRpcException).error.also {
            it.code shouldBe RpcResponseError.CODE_UPSTREAM_INVALID_RESPONSE
            it.message shouldBe "HTTP Code: 500"
        }
    }

    should("Extract error message when HTTP code is non-200") {
        val client = JsonRpcHttpClient("localhost:$port", metrics, null, null)

        val resp = """
        {    
            "jsonrpc": "2.0",
            "error": {"code": -32603, "message": "Something happened"},
            "id": 1
        }    
        """.trimIndent()
        mockServer!!.`when`(HttpRequest.request())
            .respond(HttpResponse.response().withStatusCode(500).withBody(resp))

        val t = shouldThrowAny {
            client.read(JsonRpcRequest("test", emptyList())).block(Duration.ofSeconds(1))
        }.let(Exceptions::unwrap)

        t shouldBe instanceOf<JsonRpcException>()
        (t as JsonRpcException).error.also {
            it.code shouldBe -32603
            it.message shouldBe "Something happened"
        }
    }
})
