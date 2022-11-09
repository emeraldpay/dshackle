package io.emeraldpay.dshackle.config

class EgressLogConfig(
    val enabled: Boolean = false,
    val includeMessages: Boolean = false
) {

    var filename: String = "./egress_log.jsonl"

    companion object {

        fun default(): EgressLogConfig {
            return disabled()
        }

        fun disabled(): EgressLogConfig {
            return EgressLogConfig(
                enabled = false
            )
        }
    }
}
