package io.emeraldpay.dshackle.test

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.logging.LoggingMeterRegistry

class TestingCommonsKotlin {

    companion object {
        val meterRegistry: MeterRegistry = LoggingMeterRegistry()
    }
}
