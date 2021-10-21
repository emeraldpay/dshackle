/**
 * Copyright (c) 2020 EmeraldPay, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.emeraldpay.dshackle

import io.emeraldpay.grpc.Chain

/**
 * Exception that should be handled/logged without a stacktrace in production
 */
open class SilentException(message: String) : Exception(message) {

    /**
     * Blockchain is not available or not supported by current instance of the Dshackle
     */
    class UnsupportedBlockchain(val blockchainId: Int) : SilentException("Unsupported blockchain $blockchainId") {
        constructor(chain: Chain) : this(chain.id)
    }

    class DataUnavailable(val code: String) : SilentException("Data is unavailable: $code")
}
