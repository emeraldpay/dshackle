package io.emeraldpay.dshackle.upstream

import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.json.BlockJson

interface EthereumHead: Head<BlockJson<TransactionId>> {
}