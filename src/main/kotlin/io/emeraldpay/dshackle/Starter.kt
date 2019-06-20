package io.emeraldpay.dshackle

import org.springframework.beans.factory.config.YamlPropertiesFactoryBean
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.env.YamlPropertySourceLoader
import org.springframework.context.annotation.Import
import org.springframework.context.annotation.PropertySource

@SpringBootApplication(scanBasePackages = [ "io.emeraldpay.dshackle" ])
@PropertySource("file:./dshackle.yaml", ignoreResourceNotFound = true, factory = YamlPropertySourceFactory::class)
@Import(Config::class)
open class Starter

fun main(args: Array<String>) {
    val app = SpringApplication(Starter::class.java)
    app.run(*args)
}