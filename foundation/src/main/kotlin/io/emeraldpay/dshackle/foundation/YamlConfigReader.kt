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
package io.emeraldpay.dshackle.foundation

import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.nodes.CollectionNode
import org.yaml.snakeyaml.nodes.MappingNode
import org.yaml.snakeyaml.nodes.Node
import org.yaml.snakeyaml.nodes.NodeTuple
import org.yaml.snakeyaml.nodes.ScalarNode
import java.io.InputStream
import java.io.InputStreamReader
import java.util.Locale
import kotlin.time.Duration
import kotlin.time.toJavaDuration

abstract class YamlConfigReader<T> : ConfigReader<T> {
    private val envVariables = EnvVariables()

    val filename = "dshackle.yaml"

    fun read(input: InputStream): T? {
        val configNode = readNode(input)
        return read(configNode)
    }
    fun readNode(input: String): MappingNode {
        return readNode(input.byteInputStream())
    }

    fun readNode(input: InputStream): MappingNode {
        val yaml = Yaml()
        return asMappingNode(yaml.compose(InputStreamReader(input)))
    }

    fun mergeMappingNode(a: MappingNode?, b: MappingNode?): MappingNode? = when {
        a == null -> b
        b == null -> a
        else -> {
            val mergedTuples = a.value.toMutableList()

            mergedTuples.addAll(
                b.value.filter { tupleB ->
                    mergedTuples.none { it.keyNode.valueAsString() == tupleB.keyNode.valueAsString() } && (tupleB.valueNode is MappingNode || tupleB.valueNode is ScalarNode)
                },
            )

            a.value.forEach { tupleA ->
                b.value.find { it.keyNode.valueAsString() == tupleA.keyNode.valueAsString() }?.let { tupleB ->
                    if (tupleA.valueNode is MappingNode && tupleB.valueNode is MappingNode) {
                        mergedTuples[mergedTuples.indexOf(tupleA)] = NodeTuple(tupleA.keyNode, mergeMappingNode(tupleA.valueNode as MappingNode, tupleB.valueNode as MappingNode))
                    } else if (tupleA.valueNode is ScalarNode && tupleB.valueNode is ScalarNode) {
                        mergedTuples[mergedTuples.indexOf(tupleA)] = NodeTuple(tupleA.keyNode, tupleB.valueNode)
                    }
                }
            }

            MappingNode(a.tag, mergedTuples, a.flowStyle)
        }
    }

    protected fun hasAny(mappingNode: MappingNode?, key: String): Boolean =
        mappingNode?.let { node ->
            node.value
                .stream()
                .anyMatch { it.keyNode.valueAsString() == key }
        } ?: false

    @Suppress("UNCHECKED_CAST")
    private fun <T> getValue(mappingNode: MappingNode?, key: String, type: Class<T>): T? {
        if (mappingNode == null) {
            return null
        }
        return mappingNode.value
            .stream()
            .filter { type.isAssignableFrom(it.valueNode.javaClass) }
            .filter { it.keyNode.valueAsString() == key }
            .map { n -> n.valueNode as T }
            .findFirst()
            .orElse(null)
    }

    fun Node.valueAsString(): String? = if (this is ScalarNode) this.value else null

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

    protected fun getValueAsDuration(mappingNode: MappingNode?, key: String): java.time.Duration? {
        return getValue(mappingNode, key)?.let {
            return@let if (it.isPlain) {
                Duration.parse(it.value).toJavaDuration()
            } else {
                null
            }
        }
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

    protected fun getValueAsLong(mappingNode: MappingNode?, key: String): Long? {
        return getValue(mappingNode, key)?.let {
            return@let if (it.isPlain) {
                it.value.toLongOrNull()
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

    fun getValueAsBytes(mappingNode: MappingNode?, key: String): Int? {
        return getValueAsString(mappingNode, key)?.let(envVariables::postProcess)?.let {
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
