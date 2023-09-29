package io.emeraldpay.dshackle.config

open class CompressionConfig(
    var grpc: GRPC = GRPC(),
) {
    /**
     * Config example:
     * ```
     * compression:
     *   grpc:
     *     server:
     *       enabled: true
     *     client:
     *       enabled: false
     * ```
     */
    class GRPC(
        var serverEnabled: Boolean = true,
        var clientEnabled: Boolean = true,
    )

    companion object {
        fun default(): CompressionConfig {
            return CompressionConfig()
        }
    }
}
