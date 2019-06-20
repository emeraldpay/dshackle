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