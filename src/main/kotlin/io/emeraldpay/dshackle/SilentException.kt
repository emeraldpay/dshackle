package io.emeraldpay.dshackle

import io.emeraldpay.grpc.Chain

/**
 * Exception that should be handled/logged without a stacktrace in production
 */
open class SilentException(message: String) : Exception(message) {


    /**
     * Blockchain is not available or not supported by current instance of the Dshackle
     */
    class UnsupportedBlockchain(val blockchainId: Int): SilentException("Unsupported blockchain $blockchainId") {
        constructor(chain: Chain) : this(chain.id)
    }
}