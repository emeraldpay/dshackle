package io.emeraldpay.dshackle

import io.emeraldpay.api.proto.BlockchainGrpc
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common.ChainRef
import io.emeraldpay.dshackle.config.MainConfig
import io.emeraldpay.dshackle.config.MainConfigReader
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.foundation.ChainOptions
import io.github.ganchix.ganache.Account
import io.github.ganchix.ganache.GanacheContainer
import io.grpc.BindableService
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import org.springframework.context.annotation.Profile
import org.springframework.test.context.ActiveProfiles
import org.springframework.util.ResourceUtils
import java.math.BigInteger
import java.net.URI

@SpringBootTest(properties = ["spring.main.allow-bean-definition-overriding=true"])
@Import(Config::class)
@ActiveProfiles("integration-test")
@Disabled
class IntegrationTest {

    @Autowired
    lateinit var services: List<BindableService>

    lateinit var stub: BlockchainGrpc.BlockchainBlockingStub
    companion object {

        val PRIVATE_KEY_0 = "ae020c8ddb6fbc24e167b011666639d2ce3d4aa0d9c13d02d726d6865618a781"
        val PRIVATE_KEY_1 = "81ad1ba5c4da47feb0f0163c0c61a66c4d0e6a66bd839827444b1e3362016140"

        var ganacheContainer: GanacheContainer<*> = GanacheContainer<Nothing>().apply {
            withAccounts(
                listOf(
                    Account.builder().privateKey(PRIVATE_KEY_0).balance(BigInteger.valueOf(2000000000000000000)).build(),
                    Account.builder().privateKey(PRIVATE_KEY_1).balance(BigInteger.valueOf(2000000000000000000)).build(),
                ),
            )
        }

        init {
            ganacheContainer.start()
        }
    }

    @BeforeEach
    fun prepare() {
        val serverName = InProcessServerBuilder.generateName()
        val builder = InProcessServerBuilder.forName(serverName)
            .directExecutor()
        services.forEach { builder.addService(it) }

        val managedChannel = InProcessChannelBuilder.forName(serverName).directExecutor().build()
        val server = builder.build()
        server.start()

        stub = BlockchainGrpc.newBlockingStub(managedChannel)
    }

    @Test
    fun test() {
        val result = stub.describe(BlockchainOuterClass.DescribeRequest.newBuilder().build())
        Assertions.assertThat(result.chainsCount).isEqualTo(1)
        Assertions.assertThat(result.chainsList[0].chain).isEqualTo(ChainRef.CHAIN_ETHEREUM__MAINNET)
    }

    @TestConfiguration
    open class Config {
        @Bean
        @Profile("integration-test")
        open fun mainConfig(@Autowired fileResolver: FileResolver): MainConfig {
            val reader = MainConfigReader(fileResolver)
            val config = reader.read(
                ResourceUtils.getFile("classpath:integration/dshackle.yaml")
                    .inputStream(),
            )!!
            patch(config)
            return config
        }

        private fun patch(config: MainConfig) {
            config.upstreams?.upstreams?.add(
                UpstreamsConfig.Upstream<UpstreamsConfig.EthereumPosConnection>().apply {
                    id = "ganache"
                    nodeId = 1
                    chain = "ethereum"
                    options = ChainOptions.PartialOptions()
                        .apply {
                            validateChain = false
                        }
                    connection = UpstreamsConfig.EthereumPosConnection().apply {
                        execution = UpstreamsConfig.RpcConnection().apply {
                            rpc = UpstreamsConfig.HttpEndpoint(
                                URI.create(
                                    "http://" + ganacheContainer.getHost() + ":" + ganacheContainer.getMappedPort(8545) + "/",
                                ),
                            )
                        }
                    }
                },
            )
        }
    }
}
