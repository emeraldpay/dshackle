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
package io.emeraldpay.dshackle.upstream.bitcoin

import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.HeadLagObserver
import io.emeraldpay.dshackle.upstream.Upstream
import org.slf4j.LoggerFactory

class BitcoinHeadLagObserver(
        master: Head,
        followers: Collection<Upstream>
) : HeadLagObserver(master, followers) {

    companion object {
        private val log = LoggerFactory.getLogger(BitcoinHeadLagObserver::class.java)
    }

    override fun forkDistance(top: BlockContainer, curr: BlockContainer): Long {
        //TODO fetch actual blocks
        return 3
    }

}