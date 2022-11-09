package io.emeraldpay.dshackle.monitoring.egresslog


import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.config.EgressLogConfig
import io.emeraldpay.dshackle.config.MainConfig
import io.emeraldpay.dshackle.monitoring.record.EgressRecord
import io.emeraldpay.grpc.Chain
import spock.lang.Specification

import java.time.Instant

class CurrentEgressLogWriterSpec extends Specification {

    def "writes log event"() {
        setup:
        File dir = File.createTempDir("dshackle-test-")
        File accessLog = new File(dir, "egresslog.jsonl")
        println("Write egress log to $accessLog.absolutePath")
        MainConfig config = new MainConfig()
        config.egressLogConfig = new EgressLogConfig(true, false).tap {
            it.filename = accessLog.absolutePath
        }
        CurrentEgressLogWriter logWriter = new CurrentEgressLogWriter(config)

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
