package io.emeraldpay.dshackle.test

import com.fasterxml.jackson.core.Version
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.module.SimpleModule

import java.text.SimpleDateFormat

class TestingCommons {

    static ObjectMapper objectMapper() {
        def module = new SimpleModule("EmeraldDShackle", new Version(1, 0, 0, null, null, null))

        def objectMapper = new ObjectMapper()
        objectMapper.registerModule(module)
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        objectMapper
                .setDateFormat(new SimpleDateFormat("yyyy-MM-dd\'T\'HH:mm:ss.SSS"))
                .setTimeZone(TimeZone.getTimeZone("UTC"))

        return objectMapper
    }
}
