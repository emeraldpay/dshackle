/**
 * Copyright (c) 2019 ETCDEV GmbH
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