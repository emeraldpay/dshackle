import com.squareup.kotlinpoet.*
import com.squareup.kotlinpoet.ParameterizedTypeName.Companion.parameterizedBy
import io.emeraldpay.dshackle.BlockchainType
import io.emeraldpay.dshackle.config.ChainsConfig
import io.emeraldpay.dshackle.config.ChainsConfigReader
import io.emeraldpay.dshackle.foundation.ChainOptionsReader
import java.math.BigInteger

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
                .addSuperclassConstructorParameter("%L, %S, %S, %S, %L, %L, %L", 0, "UNSPECIFIED", "Unknown", "0x0", "BigInteger.ZERO", "emptyList()", "BlockchainType.UNKNOWN")
                .build(),
        )
        for (chain in config) {
            builder.addEnumConstant(
                chain.blockchain.uppercase().replace('-', '_') + "__" + chain.id.uppercase().replace('-', '_')
                    .replace(' ', '_'),
                TypeSpec.anonymousClassBuilder()
                    .addSuperclassConstructorParameter(
                        "%L, %S, %S, %S, %L, %L, %L",
                        chain.grpcId,
                        chain.code,
                        chain.blockchain.replaceFirstChar { it.uppercase() } + " " + chain.id.replaceFirstChar { it.uppercase() },
                        chain.chainId,
                        "BigInteger(\"" + chain.netVersion + "\")",
                        "listOf(" + chain.shortNames.map { "\"${it}\"" }.joinToString() + ")",
                        type(chain.type)
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
            else -> throw IllegalArgumentException("unknown blockchain type $type")
        }
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
