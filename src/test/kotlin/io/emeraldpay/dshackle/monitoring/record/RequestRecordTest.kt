package io.emeraldpay.dshackle.monitoring.record

import io.emeraldpay.dshackle.Global
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import java.time.Instant
import java.util.UUID

class RequestRecordTest :
    ShouldSpec({

        val objectMapper = Global.objectMapper

        context("BlockchainRequest") {
            val record =
                RequestRecord.BlockchainRequest(
                    id = UUID.randomUUID(),
                    jsonrpc = null,
                    upstream = null,
                    success = true,
                    request =
                        RequestRecord.RequestDetails(
                            id = UUID.randomUUID(),
                            source = RequestRecord.Source.INTERNAL,
                            start = Instant.ofEpochMilli(1679954761969L).plusNanos(50_050),
                        ),
                    execute = Instant.ofEpochMilli(1679954761969L).plusNanos(100_050),
                    complete = Instant.ofEpochMilli(1679954761969L).plusNanos(1_120_700),
                )

            should("Produce executed queueTime with nanoseconds") {
                val json = objectMapper.writeValueAsString(record)
                println("JSON: $json")
                val queueTime = Regex("\"queueTime\":([\\d\\\\.]+)").find(json)?.groupValues?.get(1)
                queueTime shouldBe "0.05"
            }

            should("Produce non-executed queueTime with nanoseconds") {
                val json = objectMapper.writeValueAsString(record.copy(execute = null))
                println("JSON: $json")
                val queueTime = Regex("\"queueTime\":([\\d\\\\.]+)").find(json)?.groupValues?.get(1)
                queueTime shouldBe "1.07065"
            }

            should("Produce requestTime with nanoseconds") {
                val json = objectMapper.writeValueAsString(record)
                println("JSON: $json")
                val queueTime = Regex("\"requestTime\":([\\d\\\\.]+)").find(json)?.groupValues?.get(1)
                queueTime shouldBe "1.02065"
            }
        }
    })
