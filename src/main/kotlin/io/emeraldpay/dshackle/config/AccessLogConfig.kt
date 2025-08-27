package io.emeraldpay.dshackle.config

/**
 * Config for logging of the request made to the Dshackle from a client.
 */
class AccessLogConfig(
    val enabled: Boolean = false,
    val includeMessages: Boolean = false,
) {
    var target: LogTargetConfig.Any = defaultFile

    companion object {
        val defaultFile =
            LogTargetConfig.File(
                filename = "./access_log.jsonl",
            )

        fun default(): AccessLogConfig = disabled()

        fun disabled(): AccessLogConfig =
            AccessLogConfig(
                enabled = false,
            )
    }
}
