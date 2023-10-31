package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.reader.JsonRpcReader
import reactor.core.publisher.Flux

typealias LabelsDetectorBuilder = (Chain, JsonRpcReader) -> LabelsDetector?
interface LabelsDetector {
    fun detectLabels(): Flux<Pair<String, String>>
}
