package io.emeraldpay.dshackle.reader

import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.ChainResponse

typealias ChainReader = Reader<ChainRequest, ChainResponse>
