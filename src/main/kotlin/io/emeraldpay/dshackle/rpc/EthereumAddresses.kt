package io.emeraldpay.dshackle.rpc

import io.emeraldpay.api.proto.Common
import io.infinitape.etherjar.domain.Address
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux

class EthereumAddresses {

    companion object {
        private val log = LoggerFactory.getLogger(EthereumAddresses::class.java)
    }

    fun extract(addresses: Common.AnyAddress): Flux<Address> {
        return when (addresses.addrTypeCase) {
            Common.AnyAddress.AddrTypeCase.ADDRESS_SINGLE ->
                Flux.just(Address.from(addresses.addressSingle.address))
            Common.AnyAddress.AddrTypeCase.ADDRESS_MULTI ->
                Flux.fromIterable(addresses.addressMulti.addressesList)
                        .map { Address.from(it.address) }
            else -> {
                log.error("Unsupported address type: ${addresses.addrTypeCase}")
                Flux.empty()
            }
        }
    }

}