package io.emeraldpay.dshackle.config

/**
 * Config for logging of the request made to the Dshackle from a client.
 */
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
