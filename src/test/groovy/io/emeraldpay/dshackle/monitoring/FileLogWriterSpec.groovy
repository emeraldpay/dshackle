/**
 * Copyright (c) 2022 EmeraldPay, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.emeraldpay.dshackle.monitoring


import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.config.EgressLogConfig
import io.emeraldpay.dshackle.config.MainConfig
import io.emeraldpay.dshackle.monitoring.egresslog.CurrentEgressLogWriter
import io.emeraldpay.dshackle.monitoring.record.EgressRecord
import io.emeraldpay.grpc.Chain
import spock.lang.Specification

import java.time.Duration
import java.time.Instant

class FileLogWriterSpec extends Specification {

    def "writes log event"() {
        setup:
        File dir = File.createTempDir("dshackle-test-")
        File accessLog = new File(dir, "log.jsonl")
        println("Write log to $accessLog.absolutePath")

        def serializer = {
            Global.objectMapper.writeValueAsBytes(it)
        }

        def logWriter = new FileLogWriter(
                accessLog, serializer, Duration.ofMillis(10), Duration.ofMillis(10), 100
        )

        when:
        logWriter.start()
        def event = new EgressRecord.Status(
                Chain.ETHEREUM, UUID.fromString("9d8ecbf3-12fb-49cf-af9d-949a1050a000"),
                new EgressRecord.RequestDetails(
                        UUID.fromString("9d8ecbf3-12fb-49cf-af9d-949a1050a000"),
                        Instant.ofEpochMilli(1626746880123),
                        new EgressRecord.Remote(
                                ["127.0.0.1", "172.217.8.78"], "172.217.8.78", "UnitTest"
                        )
                )
        )
        logWriter.submitAll([event])
        logWriter.flush()
        def act = accessLog.readLines()
        then:
        act.size() == 1
        with(act[0]) {
            def json = Global.objectMapper.readValue(it, Map)
            json["version"] == "egresslog/v1beta2"
            json["id"] == "9d8ecbf3-12fb-49cf-af9d-949a1050a000"
            json["method"] == "Status"
            json["blockchain"] == "ETHEREUM"
            json["request"]["start"] == "2021-07-20T02:08:00.123Z"
            json["request"]["id"] == "9d8ecbf3-12fb-49cf-af9d-949a1050a000"
            json["request"]["remote"]["ip"] == "172.217.8.78"
            json["request"]["remote"]["userAgent"] == "UnitTest"
        }
    }
}
