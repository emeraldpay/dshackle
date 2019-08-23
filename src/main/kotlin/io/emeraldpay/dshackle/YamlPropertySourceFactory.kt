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

import org.apache.commons.lang3.StringUtils
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean
import org.springframework.core.env.PropertiesPropertySource
import org.springframework.core.env.PropertySource
import org.springframework.core.io.Resource
import org.springframework.core.io.support.EncodedResource
import org.springframework.core.io.support.PropertySourceFactory
import java.util.*

class YamlPropertySourceFactory: PropertySourceFactory {

    override fun createPropertySource(name: String?, resource: EncodedResource): PropertySource<*> {
        val loadedProperties = this.loadYamlIntoProperties(resource.resource)

        return PropertiesPropertySource(if (StringUtils.isNotBlank(name)) name else resource.resource.filename, loadedProperties)

    }

    private fun loadYamlIntoProperties(resource: Resource): Properties {
        val factory = YamlPropertiesFactoryBean()
        factory.setResources(resource)
        factory.afterPropertiesSet()

        return factory.getObject()
    }
}