package io.emeraldpay.dshackle.config

class AccessLogConfig(
    val enabled: Boolean = false,
    val includeMessages: Boolean = false
) {

    var filename: String = "./access_log.jsonl"

    companion object {

        fun default(): AccessLogConfig {
            return disabled()
        }

        fun disabled(): AccessLogConfig {
            return AccessLogConfig(
                enabled = false
            )
        }
    }
}
