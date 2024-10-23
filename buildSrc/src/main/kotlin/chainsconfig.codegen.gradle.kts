
import com.squareup.kotlinpoet.ClassName
import com.squareup.kotlinpoet.FileSpec
import com.squareup.kotlinpoet.FunSpec
import com.squareup.kotlinpoet.ParameterizedTypeName.Companion.parameterizedBy
import com.squareup.kotlinpoet.PropertySpec
import com.squareup.kotlinpoet.TypeSpec
import com.squareup.kotlinpoet.asClassName
import io.emeraldpay.dshackle.BlockchainType
import io.emeraldpay.dshackle.config.ChainsConfig
import io.emeraldpay.dshackle.config.ChainsConfigReader
import io.emeraldpay.dshackle.foundation.ChainOptionsReader
import java.math.BigInteger
import java.time.Duration
import kotlin.math.ceil

open class CodeGen(private val config: ChainsConfig) {
    companion object {
        fun generateFromChains(path: File) {
            val chainConfigReader = ChainsConfigReader(ChainOptionsReader())
            val config = chainConfigReader.read(null)
            CodeGen(config).generateChainsFile().writeTo(path)
        }
    }

    private fun addEnumProperties(builder: TypeSpec.Builder): TypeSpec.Builder {
        builder.addEnumConstant(
            "UNSPECIFIED",
            TypeSpec.anonymousClassBuilder()
                .addSuperclassConstructorParameter("%L, %S, %S, %S, %L, %L, %L, %L", 0, "UNSPECIFIED", "Unknown", "0x0", "BigInteger.ZERO", "emptyList()", "BlockchainType.UNKNOWN", 0.0)
                .build(),
        )
        for (chain in config) {
            builder.addEnumConstant(
                chain.blockchain.uppercase().replace('-', '_') + "__" + chain.id.uppercase().replace('-', '_')
                    .replace(' ', '_'),
                TypeSpec.anonymousClassBuilder()
                    .addSuperclassConstructorParameter(
                        "%L, %S, %S, %S, %L, %L, %L, %L",
                        chain.grpcId,
                        chain.code,
                        chain.blockchain.replaceFirstChar { it.uppercase() } + " " + chain.id.replaceFirstChar { it.uppercase() },
                        chain.chainId,
                        "BigInteger(\"" + chain.netVersion + "\")",
                        "listOf(" + chain.shortNames.map { "\"${it}\"" }.joinToString() + ")",
                        type(chain.type),
                        averageRemoveSpeed(chain.expectedBlockTime),
                    )
                    .build(),
            )
        }

        return builder
    }

    fun generateChainsFile(): FileSpec {
        val byIdFun = FunSpec.builder("byId")
            .addParameter("id", Int::class)
            .returns(ClassName("", "Chain"))
            .beginControlFlow("for (chain in values())")
            .beginControlFlow("if (chain.id == id)")
            .addStatement("return chain")
            .endControlFlow()
            .endControlFlow()
            .addStatement("return UNSPECIFIED")
            .build()

        val chainType = addEnumProperties(
            TypeSpec.enumBuilder("Chain")
                .addType(TypeSpec.companionObjectBuilder().addFunction(byIdFun).build())
                .primaryConstructor(
                    FunSpec.constructorBuilder()
                        .addParameter("id", Int::class)
                        .addParameter("chainCode", String::class)
                        .addParameter("chainName", String::class)
                        .addParameter("chainId", String::class)
                        .addParameter("netVersion", BigInteger::class)
                        .addParameter("shortNames", List::class.asClassName().parameterizedBy(String::class.asClassName()))
                        .addParameter("type", BlockchainType::class)
                        .addParameter("averageRemoveDataSpeed", Double::class.java)
                        .build(),
                )
                .addProperty(
                    PropertySpec.builder("id", Int::class)
                        .initializer("id")
                        .build(),
                )
                .addProperty(
                    PropertySpec.builder("chainCode", String::class)
                        .initializer("chainCode")
                        .build(),
                )
                .addProperty(
                    PropertySpec.builder("netVersion", BigInteger::class)
                        .initializer("netVersion")
                        .build(),
                )
                .addProperty(
                    PropertySpec.builder("chainId", String::class)
                        .initializer("chainId")
                        .build(),
                )
                .addProperty(
                    PropertySpec.builder("chainName", String::class)
                        .initializer("chainName")
                        .build(),
                )
                .addProperty(
                    PropertySpec.builder("shortNames", List::class.asClassName().parameterizedBy(String::class.asClassName()))
                        .initializer("shortNames")
                        .build(),
                )
                .addProperty(
                    PropertySpec.builder("type", BlockchainType::class)
                        .initializer("type")
                        .build(),
                )
                .addProperty(
                    PropertySpec.builder("averageRemoveDataSpeed", Double::class)
                        .initializer("averageRemoveDataSpeed")
                        .build(),
                )
        ).build()
        return FileSpec.builder("io.emeraldpay.dshackle", "Chain")
            .addType(chainType)
            .build()
    }

    private fun type(type: String): String {
        return when(type) {
            "eth" -> "BlockchainType.ETHEREUM"
            "bitcoin" -> "BlockchainType.BITCOIN"
            "starknet" -> "BlockchainType.STARKNET"
            "polkadot" -> "BlockchainType.POLKADOT"
            "solana" -> "BlockchainType.SOLANA"
            "near" -> "BlockchainType.NEAR"
            "eth-beacon-chain" -> "BlockchainType.ETHEREUM_BEACON_CHAIN"
            "ton" -> "BlockchainType.TON"
            "cosmos" -> "BlockchainType.COSMOS"
            else -> throw IllegalArgumentException("unknown blockchain type $type")
        }
    }

    private fun averageRemoveSpeed(expectedBlockTime: Duration): Double {
        return ceil(1000.0/expectedBlockTime.toMillis()*100) / 100
    }
}

open class ChainsCodeGenTask : DefaultTask() {
    init {
        group = "custom"
        description = "Generate chains config"
    }

    @TaskAction
    fun chainscodegenClass() {
        val output = project.layout.buildDirectory.dir("generated/kotlin").get().asFile
        output.mkdirs()
        CodeGen.generateFromChains(output)
    }
}

tasks.register<ChainsCodeGenTask>("chainscodegen")
