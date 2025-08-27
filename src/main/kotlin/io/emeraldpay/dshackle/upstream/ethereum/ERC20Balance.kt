/**
 * Copyright (c) 2021 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.upstream.ApiSource
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.etherjar.domain.Address
import io.emeraldpay.etherjar.erc20.ERC20Token
import io.emeraldpay.etherjar.hex.HexQuantity
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.math.BigInteger

/**
 * Query for a ERC20 token balance for an address
 */
open class ERC20Balance {
    companion object {
        private val log = LoggerFactory.getLogger(ERC20Balance::class.java)
    }

    open fun getBalance(
        upstreams: EthereumMultistream,
        token: ERC20Token,
        address: Address,
    ): Mono<BigInteger> =
        upstreams
            // use only up-to-date upstreams
            .getApiSource(Selector.HeightMatcher(upstreams.getHead().getCurrentHeight() ?: 0))
            .let { getBalance(it, token, address) }

    open fun getBalance(
        apis: ApiSource,
        token: ERC20Token,
        address: Address,
    ): Mono<BigInteger> {
        apis.request(1)
        return Flux
            .from(apis)
            .flatMap {
                getBalance(it.cast(EthereumUpstream::class.java), token, address)
            }.doOnNext {
                apis.resolve()
            }.next()
    }

    open fun getBalance(
        upstream: EthereumUpstream,
        token: ERC20Token,
        address: Address,
    ): Mono<BigInteger> =
        upstream
            .getIngressReader()
            .read(prepareEthCall(token, address, upstream.getHead()))
            .flatMap(JsonRpcResponse::requireStringResult)
            .map {
                // The value may be just `0x` when the contract doesn't exist, or if it's corrupted and its method returns nothing.
                // In this case there are two options: return null (Mono.empty()) or return 0 (BigInteger.ZERO)
                // We return BigInteger.ZERO because it's more safe for the caller that may not properly process empty / no-result,
                // also for the money zero is the same as nothing.
                HexQuantity.from(it)?.value ?: BigInteger.ZERO
            }

    fun prepareEthCall(
        token: ERC20Token,
        target: Address,
        head: Head,
    ): JsonRpcRequest {
        val call =
            token
                .readBalanceOf(target)
                .toJson()
        val height = head.getCurrentHeight()?.let { HexQuantity.from(it).toHex() } ?: "latest"
        return JsonRpcRequest("eth_call", listOf(call, height))
    }
}
