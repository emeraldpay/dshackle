/**
 * Copyright (c) 2020 EmeraldPay, Inc
 * Copyright (c) 2020 ETCDEV GmbH
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
package io.emeraldpay.dshackle.config

import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.nodes.CollectionNode
import org.yaml.snakeyaml.nodes.MappingNode
import org.yaml.snakeyaml.nodes.Node
import org.yaml.snakeyaml.nodes.ScalarNode
import java.io.InputStream
import java.io.InputStreamReader
import java.util.Locale

abstract class YamlConfigReader {
    private val envVariables = EnvVariables()

    fun readNode(input: String): MappingNode {
        return readNode(input.byteInputStream())
    }

    fun readNode(input: InputStream): MappingNode {
        val yaml = Yaml()
        return asMappingNode(yaml.compose(InputStreamReader(input)))
    }

    protected fun hasAny(mappingNode: MappingNode?, vararg key: String): Boolean {
        if (mappingNode == null) {
            return false
        }
        return mappingNode.value
            .stream()
            .filter { n -> n.keyNode is ScalarNode }
            .filter { n ->
                val sn = n.keyNode as ScalarNode
                key.any { it == sn.value }
            }.count() > 0
    }

    @Suppress("UNCHECKED_CAST")
    private fun <T> getValue(mappingNode: MappingNode?, key: String, type: Class<T>): T? {
        if (mappingNode == null) {
            return null
        }
        return mappingNode.value
            .stream()
            .filter { n -> n.keyNode is ScalarNode && type.isAssignableFrom(n.valueNode.javaClass) }
            .filter { n ->
                val sn = n.keyNode as ScalarNode
                key == sn.value
            }
            .map { n -> n.valueNode as T }
            .findFirst().let {
                if (it.isPresent) {
                    it.get()
                } else {
                    null
                }
            }
    }

    protected fun getMapping(mappingNode: MappingNode?, vararg keys: String): MappingNode? {
        return keys
            .find { key -> hasAny(mappingNode, key) }
            ?.let { key -> getValue(mappingNode, key, MappingNode::class.java) }
    }

    private fun getValue(mappingNode: MappingNode?, key: String): ScalarNode? {
        return getValue(mappingNode, key, ScalarNode::class.java)
    }

    @Suppress("UNCHECKED_CAST")
    protected fun <T> getList(mappingNode: MappingNode?, vararg keys: String): CollectionNode<T>? {
        return keys
            .find { key -> hasAny(mappingNode, key) }
            ?.let { key ->
                getValue(mappingNode, key, CollectionNode::class.java) as CollectionNode<T>
            }
    }

    protected fun getListOfString(mappingNode: MappingNode?, vararg keys: String): List<String>? {
        return keys
            .find { key -> hasAny(mappingNode, key) }
            ?.let { key ->
                getList<ScalarNode>(mappingNode, key)?.value
                    ?.map { it.value }
                    ?.map(envVariables::postProcess)
            }
    }

    protected fun getValueAsString(mappingNode: MappingNode?, vararg keys: String): String? {
        return keys
            .find { key -> hasAny(mappingNode, key) }
            ?.let { key ->
                getValue(mappingNode, key)
                    ?.value
                    ?.let(envVariables::postProcess)
            }
    }

    protected fun getValueAsInt(mappingNode: MappingNode?, vararg keys: String): Int? {
        return keys
            .find { key -> hasAny(mappingNode, key) }
            ?.let { key ->
                getValue(mappingNode, key)?.let {
                    if (it.isPlain) {
                        it.value.toIntOrNull()
                    } else {
                        null
                    }
                }
            }
    }

    protected fun getValueAsBool(mappingNode: MappingNode?, vararg keys: String): Boolean? {
        return keys
            .find { key -> hasAny(mappingNode, key) }
            ?.let { key ->
                getValue(mappingNode, key)?.let {
                    if (it.isPlain) {
                        it.value.lowercase(Locale.getDefault()) == "true"
                    } else {
                        null
                    }
                }
            }
    }

    protected fun asMappingNode(node: Node): MappingNode {
        return if (MappingNode::class.java.isAssignableFrom(node.javaClass)) {
            node as MappingNode
        } else {
            throw IllegalArgumentException("Not a map")
        }
    }

    fun getValueAsBytes(mappingNode: MappingNode?, vararg keys: String): Int? {
        return keys
            .find { key -> hasAny(mappingNode, key) }
            ?.let { key ->
                getValueAsString(mappingNode, key)?.let(envVariables::postProcess)?.let {
                    val m = Regex("^(\\d+)(m|mb|k|kb|b)?$").find(it.lowercase().trim())
                        ?: throw IllegalArgumentException("Not a data size: $it. Example of correct values: '1024', '1kb', '5mb'")
                    val multiplier = m.groups[2]?.let {
                        when (it.value) {
                            "k", "kb" -> 1024
                            "m", "mb" -> 1024 * 1024
                            else -> 1
                        }
                    } ?: 1
                    val base = m.groups[1]!!.value.toInt()
                    base * multiplier
                }
            }
    }
}
