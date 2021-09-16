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

import io.emeraldpay.grpc.Chain
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.nodes.CollectionNode
import org.yaml.snakeyaml.nodes.MappingNode
import org.yaml.snakeyaml.nodes.Node
import org.yaml.snakeyaml.nodes.ScalarNode
import java.io.InputStream
import java.io.InputStreamReader
import java.util.*

abstract class YamlConfigReader {
    private val envVariables = EnvVariables()

    fun readNode(input: String): MappingNode {
        return readNode(input.byteInputStream())
    }

    fun readNode(input: InputStream): MappingNode {
        val yaml = Yaml()
        return asMappingNode(yaml.compose(InputStreamReader(input)))
    }

    protected fun hasAny(mappingNode: MappingNode?, key: String): Boolean {
        if (mappingNode == null) {
            return false
        }
        return mappingNode.value
                .stream()
                .filter { n -> n.keyNode is ScalarNode }
                .filter { n ->
                    val sn = n.keyNode as ScalarNode
                    key == sn.value
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

    protected fun getMapping(mappingNode: MappingNode?, key: String): MappingNode? {
        return getValue(mappingNode, key, MappingNode::class.java)
    }

    private fun getValue(mappingNode: MappingNode?, key: String): ScalarNode? {
        return getValue(mappingNode, key, ScalarNode::class.java)
    }

    @Suppress("UNCHECKED_CAST")
    protected fun <T> getList(mappingNode: MappingNode?, key: String): CollectionNode<T>? {
        val value = getValue(mappingNode, key, CollectionNode::class.java) ?: return null
        return value as CollectionNode<T>
    }

    protected fun getListOfString(mappingNode: MappingNode?, key: String): List<String>? {
        return getList<ScalarNode>(mappingNode, key)?.value
                ?.map { it.value }
                ?.map(envVariables::postProcess)
    }

    protected fun getValueAsString(mappingNode: MappingNode?, key: String): String? {
        return getValue(mappingNode, key)?.let {
            return@let it.value
        }?.let(envVariables::postProcess)
    }

    protected fun getValueAsInt(mappingNode: MappingNode?, key: String): Int? {
        return getValue(mappingNode, key)?.let {
            return@let if (it.isPlain) {
                it.value.toIntOrNull()
            } else {
                null
            }
        }
    }

    protected fun getValueAsBool(mappingNode: MappingNode?, key: String): Boolean? {
        return getValue(mappingNode, key)?.let {
            return@let if (it.isPlain) {
                it.value.lowercase(Locale.getDefault()) == "true"
            } else {
                null
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

    // ----

    fun getBlockchain(id: String): Chain {
        return Chain.values().find { chain ->
            chain.name == id.uppercase(Locale.getDefault())
                    || chain.chainCode.uppercase(Locale.getDefault()) == id.uppercase(Locale.getDefault())
                    || chain.id.toString() == id
        } ?: Chain.UNSPECIFIED
    }
}