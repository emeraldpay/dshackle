package io.emeraldpay.dshackle.rpc

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.BlockchainType
import io.emeraldpay.dshackle.SilentException
import io.emeraldpay.dshackle.config.TokensConfig
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.MultistreamHolder
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.ethereum.EthereumMultistream
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.domain.Address
import io.infinitape.etherjar.erc20.ERC20Token
import io.infinitape.etherjar.hex.Hex32
import io.infinitape.etherjar.hex.HexQuantity
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.math.BigInteger
import javax.annotation.PostConstruct

@Service
class TrackERC20Address(
        @Autowired private val multistreamHolder: MultistreamHolder,
        @Autowired private val tokensConfig: TokensConfig
) : TrackAddress {

    companion object {
        private val log = LoggerFactory.getLogger(TrackERC20Address::class.java)
    }

    private val ethereumAddresses = EthereumAddresses()
    private val tokens: MutableMap<TokenId, TokenDefinition> = HashMap()

    @PostConstruct
    fun init() {
        tokensConfig.tokens.forEach { token ->
            val chain = token.blockchain!!
            val asset = token.name!!.toLowerCase()
            val id = TokenId(chain, asset)
            val definition = TokenDefinition(
                    chain, asset,
                    ERC20Token(Address.from(token.address))
            )
            tokens[id] = definition
            log.info("Enable ERC20 balance for $chain:$asset")
        }
    }

    override fun isSupported(chain: Chain, asset: String): Boolean {
        return tokens.containsKey(TokenId(chain, asset.toLowerCase())) &&
                BlockchainType.fromBlockchain(chain) == BlockchainType.ETHEREUM && multistreamHolder.isAvailable(chain)
    }

    override fun getBalance(request: BlockchainOuterClass.BalanceRequest): Flux<BlockchainOuterClass.AddressBalance> {
        val chain = Chain.byId(request.asset.chainValue)
        val asset = request.asset.code.toLowerCase()
        val tokenDefinition = tokens[TokenId(chain, asset)] ?: return Flux.empty()
        return ethereumAddresses.extract(request.address)
                .map { TrackedAddress(chain, it, tokenDefinition.token, tokenDefinition.name) }
                .flatMap { addr -> getBalance(addr).map(addr::withBalance) }
                .map { buildResponse(it) }
    }

    override fun subscribe(request: BlockchainOuterClass.BalanceRequest): Flux<BlockchainOuterClass.AddressBalance> {
        val chain = Chain.byId(request.asset.chainValue)
        val asset = request.asset.code.toLowerCase()
        val tokenDefinition = tokens[TokenId(chain, asset)] ?: return Flux.empty()
        val head = multistreamHolder.getUpstream(chain)?.getHead()?.getFlux() ?: Flux.empty()

        return ethereumAddresses.extract(request.address)
                .map { TrackedAddress(chain, it, tokenDefinition.token, tokenDefinition.name) }
                .flatMap { addr ->
                    val current = getBalance(addr)
                    val updates = head.flatMap { getBalance(addr) }
                    Flux.concat(current, updates)
                            .distinctUntilChanged()
                            .map { addr.withBalance(it) }
                }
                .map { buildResponse(it) }
    }

    fun getBalance(addr: TrackedAddress): Mono<BigInteger> {
        val upstream = getUpstream(addr.chain)
        return upstream
                .getDirectApi(Selector.empty)
                .flatMap { api ->
                    api.read(prepareEthCall(addr.token, addr.address, upstream.getHead()))
                            .flatMap(JsonRpcResponse::requireStringResult)
                            .map {
                                Hex32.from(it).asQuantity().value
                            }
                }
    }

    fun prepareEthCall(token: ERC20Token, target: Address, head: Head): JsonRpcRequest {
        val call = token
                .readBalanceOf(target)
                .toJson()
        val height = head.getCurrentHeight()?.let { HexQuantity.from(it).toHex() } ?: "latest"
        return JsonRpcRequest("eth_call", listOf(call, height))
    }

    fun getUpstream(chain: Chain): EthereumMultistream {
        return multistreamHolder.getUpstream(chain)?.cast(EthereumMultistream::class.java)
                ?: throw SilentException.UnsupportedBlockchain(chain)
    }

    private fun buildResponse(address: TrackedAddress): BlockchainOuterClass.AddressBalance {
        return BlockchainOuterClass.AddressBalance.newBuilder()
                .setBalance(address.balance!!.toString(10))
                .setAsset(Common.Asset.newBuilder()
                        .setChainValue(address.chain.id)
                        .setCode(address.tokenName.toUpperCase()))
                .setAddress(Common.SingleAddress.newBuilder().setAddress(address.address.toHex()))
                .build()
    }

    class TrackedAddress(val chain: Chain,
                         val address: Address,
                         val token: ERC20Token,
                         val tokenName: String,
                         val balance: BigInteger? = null
    ) {
        fun withBalance(balance: BigInteger) = TrackedAddress(chain, address, token, tokenName, balance)
    }

    data class TokenId(val chain: Chain, val name: String)
    data class TokenDefinition(val chain: Chain, val name: String, val token: ERC20Token)

}