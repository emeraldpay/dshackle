package io.emeraldpay.dshackle.upstream.rpcclient

import io.emeraldpay.dshackle.config.AuthConfig
import io.emeraldpay.dshackle.test.NoOpChannelMetricsRecorder
import io.emeraldpay.dshackle.test.TestingCommonsKotlin
import io.emeraldpay.etherjar.rpc.RpcResponseError
import io.kotest.assertions.throwables.shouldThrowAny
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.comparables.shouldBeGreaterThan
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.instanceOf
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Timer
import org.mockserver.configuration.Configuration
import org.mockserver.integration.ClientAndServer
import org.mockserver.model.Delay
import org.mockserver.model.HttpRequest
import org.mockserver.model.HttpResponse
import org.mockserver.model.MediaType
import reactor.core.Exceptions
import reactor.core.scheduler.Schedulers
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import org.springframework.test.util.TestSocketUtils

class JsonRpcHttpClientTest : ShouldSpec({

    val port = TestSocketUtils.findAvailableTcpPort()
    val metrics = RpcMetrics(
        emptyList<Tag>(),
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

    should("Handle many requests") {
        if (System.getenv("CI") == "true") {
            // skip on CI, it's too slow
            println("Skipping test on CI")
            return@should
        }
        mockServer?.stop()
        mockServer = ClientAndServer.startClientAndServer(
            Configuration().also {
                it.actionHandlerThreadCount(100)
                it.nioEventLoopThreadCount(100)
            },
            port
        )
        val client = JsonRpcHttpClient("localhost:$port", metrics, null, null)
        val resp = """
        {    
            "jsonrpc": "2.0",
            "result": 100,
            "id": 1
        }    
        """.trimIndent()
        mockServer!!.`when`(HttpRequest.request())
            .respond(
                HttpResponse.response()
                    .withDelay(Delay.milliseconds(1500))
                    .withStatusCode(200)
                    .withBody(resp)
            )

//        val executor = Executors.newFixedThreadPool(2000)
//        val count = AtomicInteger(0)
//        repeat(2000) { id ->
//            executor.execute {
//                try {
//                    client.read(JsonRpcRequest("test", emptyList()))
//                        .subscribeOn(Schedulers.immediate())
//                        .block(Duration.ofSeconds(5))
//                    count.incrementAndGet()
//                } catch (t: Throwable) {
//                    val e = Exceptions.unwrap(t)
//                    if (e.message.equals("Connection prematurely closed BEFORE response")) {
//                        // ignore, may happen when the Executor is closing
//                    } else {
//                        System.err.println("Error for call ${id}: ${e.message}")
//                    }
//                }
//            }
//        }
//        executor.shutdown()
//        executor.awaitTermination(60, TimeUnit.SECONDS)

        val wait = CountDownLatch(2000)
        val executor = Schedulers.fromExecutor(Executors.newFixedThreadPool(2000))
        val count = AtomicInteger(0)
        repeat(2000) { id ->
            client.read(JsonRpcRequest("test", emptyList()))
                .subscribeOn(executor)
                .doFinally { wait.countDown() }
                .doOnError { e ->
                    if (e.message.equals("Connection prematurely closed BEFORE response")) {
                        // ignore, may happen when the Executor is closing
                    } else {
                        System.err.println("Error for call $id: ${e.message}")
                    }
                }
                .subscribe {
                    count.incrementAndGet()
                }
        }
        wait.await(60, TimeUnit.SECONDS)

        println("Count: ${count.get()}")
        // a few connection may fail, especially when running on a CI, so check that at least majority of requests were successful
        count.get() shouldBeGreaterThan 1950
    }
})
