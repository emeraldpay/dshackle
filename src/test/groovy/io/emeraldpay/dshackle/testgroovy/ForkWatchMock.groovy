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
package io.emeraldpay.dshackle.testgroovy

import io.emeraldpay.dshackle.upstream.ForkWatch
import io.emeraldpay.dshackle.upstream.NeverForkChoice
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.api.Chain
import org.jetbrains.annotations.NotNull
import reactor.core.publisher.Flux

class ForkWatchMock extends ForkWatch {

    private Flux<Boolean> results

    ForkWatchMock(Flux<Boolean> results) {
        super(new NeverForkChoice(), Chain.ETHEREUM)
        this.results = results
    }

    @Override
    Flux<Boolean> register(@NotNull Upstream upstream) {
        return results
    }
}
