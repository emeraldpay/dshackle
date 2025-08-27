/**
 * Copyright (c) 2023 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.commons

import java.time.Duration
import java.util.concurrent.atomic.AtomicLong

/**
 * A wrapper for a potentially too frequent event that limits its rate to max withing the specified duration.
 */
class RateLimitedAction(
    period: Duration,
) {
    private val lastRun = AtomicLong(0)
    private val runRate = period.toMillis()

    fun execute(block: () -> Unit) {
        val now = System.currentTimeMillis()
        val lock =
            lastRun.updateAndGet { prev ->
                if (prev + runRate <= now) {
                    now
                } else {
                    prev
                }
            }
        if (lock == now) {
            block.invoke()
        }
    }
}
