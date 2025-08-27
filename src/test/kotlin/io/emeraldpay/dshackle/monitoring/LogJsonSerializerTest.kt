package io.emeraldpay.dshackle.monitoring

import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.monitoring.record.AccessRecord
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.string.shouldHaveMinLength
import java.time.Instant

class LogJsonSerializerTest :
    ShouldSpec({

        should("serialize a message") {
            val message =
                AccessRecord.RequestDetails(
                    id = java.util.UUID.fromString("ad6d16d1-8148-4dbe-b4ed-ce345511d078"),
                    start = Instant.parse("2023-10-11T12:13:14Z"),
                    remote = null,
                )
            val serializer = LogJsonSerializer<AccessRecord.RequestDetails>()

            val buffer = serializer.apply(message)
            val act = MonitoringTestCommons.bufferToString(buffer)

            act shouldBe
                """
                {"id":"ad6d16d1-8148-4dbe-b4ed-ce345511d078","start":"2023-10-11T12:13:14Z"}
                """.trimIndent()
        }

        should("serialize a long message") {
            val message =
                AccessRecord.RequestDetails(
                    id = java.util.UUID.fromString("ad6d16d1-8148-4dbe-b4ed-ce345511d078"),
                    start = Instant.parse("2023-10-11T12:13:14Z"),
                    remote =
                        AccessRecord.Remote(
                            ips = listOf("127.0.0.1", "10.0.3.1", "10.23.51.11", "100.101.102.103"),
                            ip = "100.101.102.103",
                            userAgent =
                                "Lorem ipsum dolor sit amet nulla magna occaecat tempor enim laborum mollit " +
                                    "aliquip minim Lorem id, culpa ex aliqua. Consequat do dolor consequat anim magna " +
                                    "veniam enim quis cupidatat duis, aute fugiat nisi officia mollit reprehenderit aute. " +
                                    "Commodo nisi veniam incididunt consectetur proident non consectetur sunt laborum eu " +
                                    "reprehenderit nisi mollit laboris minim Lorem irure, ad aliquip. Nostrud ut enim nulla " +
                                    "labore amet, enim veniam in labore eiusmod elit anim nisi duis pariatur.",
                        ),
                )
            val serializer = LogJsonSerializer<AccessRecord.RequestDetails>(32)

            val buffer = serializer.apply(message)
            val act = MonitoringTestCommons.bufferToString(buffer)

            act shouldHaveMinLength 32
            act shouldContain "Lorem ipsum"
            act shouldContain "magna veniam"

            val parsed = Global.objectMapper.readValue(act, AccessRecord.RequestDetails::class.java)
            parsed shouldBe message
        }
    })
