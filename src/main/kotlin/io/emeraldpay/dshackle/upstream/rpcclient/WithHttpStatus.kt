package io.emeraldpay.dshackle.upstream.rpcclient

import java.util.function.Consumer

/**
 * Interface for RPC clients that can notify about HTTP status codes
 */
interface WithHttpStatus {

    /**
     * Called for each HTTP error code (non-200)
     */
    var onHttpError: Consumer<Int>?
}
