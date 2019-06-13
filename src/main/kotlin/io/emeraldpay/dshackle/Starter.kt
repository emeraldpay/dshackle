package io.emeraldpay.dshackle

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Import

@SpringBootApplication(scanBasePackages = [ "io.emeraldpay.dshackle" ])
@Import(Config::class)
open class Starter

fun main(args: Array<String>) {
    val app = SpringApplication(Starter::class.java)
    app.run(*args)
}