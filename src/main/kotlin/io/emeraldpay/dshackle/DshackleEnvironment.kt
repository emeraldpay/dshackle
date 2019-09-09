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

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean
import org.springframework.core.env.*
import org.springframework.core.io.FileSystemResource
import org.springframework.core.io.Resource
import java.io.File
import java.util.*

const val DEFAULT_CONFIG = "/etc/dshackle/dshackle.yaml"
const val LOCAL_CONFIG = "./dshackle.yaml"

open class DshackleEnvironment: StandardEnvironment() {

    companion object {
        private val log = LoggerFactory.getLogger(DshackleEnvironment::class.java)
    }


    override fun customizePropertySources(propertySources: MutablePropertySources) {
        super.customizePropertySources(propertySources)
        propertySources.addLast(mainConfig())
    }

    open fun mainConfig(): PropertySource<*> {
        var target = File(DEFAULT_CONFIG)
        if (!isAcceptedConfig(target)) {
            target = File(LOCAL_CONFIG)
            if (!isAcceptedConfig(target)) {
                log.error("Configuration is not found neither at $DEFAULT_CONFIG nor $LOCAL_CONFIG")
                return PropertySource.named("mainConfig")
            }
        }
        target = target.normalize()
        log.info("Load configuration from: ${target.absolutePath}")
        val loadedProperties = this.loadYaml(FileSystemResource(target))
        loadedProperties["configPath"] = target.absolutePath
        return PropertiesPropertySource("mainConfig", loadedProperties)
    }

    protected fun loadYaml(resource: Resource): Properties {
        val factory = YamlPropertiesFactoryBean()
        factory.setResources(resource)
        factory.afterPropertiesSet()
        return factory.getObject()!!
    }

    protected fun isAcceptedConfig(target: File): Boolean {
        return target.exists() && target.isFile
    }
}