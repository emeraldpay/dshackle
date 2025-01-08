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

const val ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

class EthereumUpstreamSettingsDetector(
    private val _upstream: Upstream,
    private val chain: Chain,
) : BasicEthUpstreamSettingsDetector(_upstream) {
    private val blockNumberReader = EthereumArchiveBlockNumberReader(upstream.getIngressReader())
    private val notArchived = upstream
        .getLabels()
        .find { it.getOrDefault("archive", "") == "false" } != null

    override fun internalDetectLabels(): Flux<Pair<String, String>> {
        return Flux.merge(
            detectNodeType(),
            detectArchiveNode(notArchived),
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
                            "code" to "0x6080604052348015600f57600080fd5b506004361060285760003560e01c806351be4eaa14602d575b600080fd5b60336047565b604051603e91906066565b60405180910390f35b60005a905090565b6000819050919050565b606081604f565b82525050565b6000602082019050607960008301846059565b9291505056fea26469706673582212201c0202887c1afe66974b06ee355dee07542bbc424cf4d1659c91f56c08c3dcc064736f6c63430008130033",
                        ),
                    ),
                ),
            ),
        ).flatMap {
            it.requireResult()
        }.flatMapMany {
            val gaslimit = String(it).drop(3).dropLast(1).toBigInteger(16) + (21182).toBigInteger()
            val nodeGasLimit = gaslimit.toString(10)
            val labels = mutableListOf(Pair("gas-limit", nodeGasLimit))
            if (gaslimit.toLong() > 590_000_000L) {
                labels.add(Pair("extra_gas_limit", 600_000_000.toString()))
            } else {
                // disable extra gas
                labels.add(Pair("extra_gas_limit", nodeGasLimit))
            }
            Flux.fromIterable(labels)
        }.onErrorResume {
            Flux.empty()
        }
    }

    private fun detectArchiveNode(notArchived: Boolean): Mono<Pair<String, String>> {
        if (notArchived) {
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
