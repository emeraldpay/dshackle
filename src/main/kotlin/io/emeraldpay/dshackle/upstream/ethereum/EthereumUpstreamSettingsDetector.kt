package io.emeraldpay.dshackle.upstream.ethereum

import com.fasterxml.jackson.databind.JsonNode
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.upstream.BasicEthUpstreamSettingsDetector
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.ChainResponse
import io.emeraldpay.dshackle.upstream.NodeTypeRequest
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import kotlin.text.toBigInteger

const val ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

class EthereumUpstreamSettingsDetector(
    private val _upstream: Upstream,
    private val chain: Chain,
) : BasicEthUpstreamSettingsDetector(_upstream) {
    private val blockNumberReader = EthereumArchiveBlockNumberReader(upstream.getIngressReader())

    override fun detectLabels(): Flux<Pair<String, String>> {
        return Flux.merge(
            detectNodeType(),
            detectArchiveNode(),
            detectGasLabels(),
        )
    }

    override fun mapping(node: JsonNode): String {
        return node.asText()
    }

    override fun clientVersionRequest(): ChainRequest {
        return ChainRequest("web3_clientVersion", ListParams())
    }

    override fun parseClientVersion(data: ByteArray): String {
        val version = String(data)
        if (version.startsWith("\"") && version.endsWith("\"")) {
            return version.substring(1, version.length - 1)
        }
        return version
    }

    /**
     * We use this smart contract to get gas limit
     * pragma solidity ^0.8.0;
     *
     * contract GasChecker {
     *
     *     // Function to return the amount of gas left
     *     function getGasLeft() external view returns (uint256) {
     *         return gasleft();
     *     }
     * }
     *
     */
    private fun detectGasLabels(): Flux<Pair<String, String>> {
        return upstream.getIngressReader().read(
            ChainRequest(
                "eth_call",
                ListParams(
                    mapOf(
                        "to" to "0x53Daa71B04d589429f6d3DF52db123913B818F22",
                        "data" to "0x51be4eaa",
                    ),
                    "latest",
                    mapOf(
                        "0x53Daa71B04d589429f6d3DF52db123913B818F22" to mapOf(
                            "code" to "0x6080604052348015600e575f80fd5b50600436106026575f3560e01c806351be4eaa14602a575b5f80fd5b60306044565b604051603b91906061565b60405180910390f35b5f5a905090565b5f819050919050565b605b81604b565b82525050565b5f60208201905060725f8301846054565b9291505056fea2646970667358221220a85b088da3911ea743505594ac7cfdd1a65865de64499ee1f3c6bd9cdad4552364736f6c634300081a0033",
                        ),
                    ),
                ),
            ),
        ).flatMap {
            it.requireResult()
        }.flatMapMany {
            val gaslimit = String(it).drop(3).dropLast(1).toBigInteger(16) + (21180).toBigInteger()
            val labels = mutableListOf(Pair("gas-limit", gaslimit.toString(10)))
            if (gaslimit >= (600_000_000).toBigInteger()) {
                labels.add(Pair("extra_gas_limit", "600000000"))
            }
            Flux.fromIterable(labels)
        }.onErrorResume {
            Flux.empty()
        }
    }

    private fun detectArchiveNode(): Mono<Pair<String, String>> {
        if (upstream.getLabels().firstOrNull { it.getOrDefault("archive", "") == "false" } != null) {
            return Mono.empty()
        }
        return Mono.zip(
            blockNumberReader.readEarliestBlock(chain).flatMap { haveBalance(it) },
            blockNumberReader.readArchiveBlock().flatMap { haveBalance(it) },
        )
            .map { "archive" to "true" }
            .onErrorResume { Mono.just("archive" to "false") }
    }

    private fun haveBalance(blockNumber: String): Mono<ByteArray> {
        return upstream.getIngressReader().read(
            ChainRequest(
                "eth_getBalance",
                ListParams(ZERO_ADDRESS, blockNumber),
            ),
        )
            .flatMap(ChainResponse::requireResult)
            .doOnNext {
                if (it.contentEquals(Global.nullValue)) {
                    throw IllegalStateException("Null data")
                }
            }
    }

    override fun nodeTypeRequest(): NodeTypeRequest = NodeTypeRequest(clientVersionRequest())
}
