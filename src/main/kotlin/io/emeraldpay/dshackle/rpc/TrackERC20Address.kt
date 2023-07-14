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
package io.emeraldpay.dshackle.rpc

import io.emeraldpay.api.BlockchainType
import io.emeraldpay.api.Chain
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.SilentException
import io.emeraldpay.dshackle.config.TokensConfig
import io.emeraldpay.dshackle.upstream.MultistreamHolder
import io.emeraldpay.dshackle.upstream.ethereum.ERC20Balance
import io.emeraldpay.dshackle.upstream.ethereum.EthereumMultistream
import io.emeraldpay.etherjar.domain.Address
import io.emeraldpay.etherjar.domain.EventId
import io.emeraldpay.etherjar.erc20.ERC20Token
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.math.BigInteger
import java.util.Locale
import javax.annotation.PostConstruct

@Service
class TrackERC20Address(
    @Autowired private val multistreamHolder: MultistreamHolder,
    @Autowired private val tokensConfig: TokensConfig
) : TrackAddress {

    companion object {
        private val log = LoggerFactory.getLogger(TrackERC20Address::class.java)
    }

    var erc20Balance: ERC20Balance = ERC20Balance()

    private val ethereumAddresses = EthereumAddresses()
    private val tokens: MutableMap<TokenId, TokenDefinition> = HashMap()

    @PostConstruct
    fun init() {
        tokensConfig.tokens.forEach { token ->
            val chain = token.blockchain!!
            val asset = token.name!!.lowercase(Locale.getDefault())
            val id = TokenId(chain, asset)
            val definition = TokenDefinition(
                chain, asset,
                ERC20Token(Address.from(token.address))
            )
            tokens[id] = definition
            log.info("Enable ERC20 balance for $chain:$asset")
        }
    }

    override fun isSupported(request: BlockchainOuterClass.BalanceRequest): Boolean {
        val chain = request.getAnyAssetChain()
        if (BlockchainType.from(chain) != BlockchainType.ETHEREUM) {
            return false
        }
        if (!multistreamHolder.isAvailable(chain)) {
            return false
        }
        if (request.hasAsset()) {
            // supports registered tokens by token name
            val asset = request.asset.code.lowercase(Locale.getDefault())
            return tokens.containsKey(TokenId(chain, asset))
        }
        if (request.hasErc20Asset()) {
            // supports any valid address as contract address
            val address = request.erc20Asset.contractAddress.lowercase(Locale.getDefault())
            return Address.isValidAddress(address)
        }
        return false
    }

    override fun getBalance(request: BlockchainOuterClass.BalanceRequest): Flux<BlockchainOuterClass.AddressBalance> {
        val chain = request.getAnyAssetChain()
        val tokenDefinition = tokenDefinition(chain, request) ?: return Flux.empty()
        return ethereumAddresses.extract(request.address)
            .map { TrackedAddress(chain, it, tokenDefinition.token, tokenDefinition.name) }
            .flatMap { addr -> getBalance(addr).map(addr::withBalance) }
            .map { buildResponse(it) }
    }

    private fun tokenDefinition(chain: Chain, request: BlockchainOuterClass.BalanceRequest): TokenDefinition? {
        if (request.hasAsset()) {
            val asset = request.asset.code.lowercase(Locale.getDefault())
            return tokens[TokenId(chain, asset)]
        }
        if (request.hasErc20Asset()) {
            val asset = request.erc20Asset.contractAddress.lowercase(Locale.getDefault())
            val token = ERC20Token(Address.from(asset))
            return TokenDefinition(chain, null, token)
        }
        throw IllegalArgumentException("Neither asset nor erc20Asset is specified")
    }

    override fun subscribe(request: BlockchainOuterClass.BalanceRequest): Flux<BlockchainOuterClass.AddressBalance> {
        val chain = request.getAnyAssetChain()
        val tokenDefinition = tokenDefinition(chain, request) ?: return Flux.empty()
        val logs = getUpstream(chain)
            .getEgressSubscription().logs
            .create(
                listOf(tokenDefinition.token.contract),
                listOf(EventId.fromSignature("Transfer", "address", "address", "uint256"))
            )
            .connect()

        return ethereumAddresses.extract(request.address)
            .map { TrackedAddress(chain, it, tokenDefinition.token, tokenDefinition.name) }
            .flatMap { addr ->
                val current = getBalance(addr)

                val updates = logs
                    .filter {
                        try {
                            it.topics.size >= 3 && (Address.extract(it.topics[1]) == addr.address || Address.extract(it.topics[2]) == addr.address)
                        } catch (e: Exception) {
                            // technically it's possible to construct an invalid event that would crash the previous step, so we just ignore those events
                            log.debug("Invalid event {}", it.transactionHash)
                            false
                        }
                    }
                    .distinctUntilChanged {
                        // check it once per block
                        it.blockHash
                    }
                    .flatMap {
                        // make sure we use actual balance, don't trust the event blindly
                        getBalance(addr)
                    }
                Flux.concat(current, updates)
                    .distinctUntilChanged()
                    .map { addr.withBalance(it) }
            }
            .doOnError { t -> log.error("Failed to process subscription to ERC20 balance", t) }
            .map { buildResponse(it) }
    }

    fun getBalance(addr: TrackedAddress): Mono<BigInteger> {
        val upstream = getUpstream(addr.chain)
        return erc20Balance.getBalance(upstream, addr.token, addr.address)
    }

    fun getUpstream(chain: Chain): EthereumMultistream {
        return multistreamHolder.getUpstream(chain)?.cast(EthereumMultistream::class.java)
            ?: throw SilentException.UnsupportedBlockchain(chain)
    }

    private fun buildResponse(address: TrackedAddress): BlockchainOuterClass.AddressBalance {
        val builder = BlockchainOuterClass.AddressBalance.newBuilder()
            .setBalance(address.balance!!.toString(10))
            .setAddress(Common.SingleAddress.newBuilder().setAddress(address.address.toHex()))

        if (address.tokenName != null) {
            builder.setAsset(
                Common.Asset.newBuilder()
                    .setChainValue(address.chain.id)
                    .setCode(address.tokenName.uppercase(Locale.getDefault()))
            )
        } else {
            builder.setErc20Asset(
                Common.Erc20Asset.newBuilder()
                    .setChainValue(address.chain.id)
                    .setContractAddress(address.token.contract.toHex())
            )
        }

        return builder.build()
    }

    data class TrackedAddress(
        val chain: Chain,
        val address: Address,
        val token: ERC20Token,
        val tokenName: String? = null,
        val balance: BigInteger? = null
    ) {
        fun withBalance(balance: BigInteger) = copy(balance = balance)
    }

    data class TokenId(val chain: Chain, val name: String)
    data class TokenDefinition(val chain: Chain, val name: String?, val token: ERC20Token)
}
