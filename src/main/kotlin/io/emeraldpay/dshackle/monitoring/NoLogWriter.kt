/**
 * Copyright (c) 2022 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.monitoring

class NoLogWriter<T> : LogWriter<T> {

    private var started: Boolean = false

    override fun submit(event: T) {
    }

    override fun submitAll(events: List<T>) {
    }

    override fun start() {
        started = true
    }

    override fun stop() {
        started = false
    }

    override fun isRunning(): Boolean {
        return started
    }
}
