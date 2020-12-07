/**
 * Copyright (c) 2020 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.upstream

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.dshackle.config.UpstreamsConfig
import org.apache.commons.lang3.StringUtils
import java.util.*
import kotlin.collections.ArrayList

class Selector {

    companion object {

        val empty = EmptyMatcher()

        @JvmStatic
        fun convertToMatcher(req: BlockchainOuterClass.Selector?): LabelSelectorMatcher {
            return when {
                req == null -> AnyLabelMatcher()
                req.hasLabelSelector() -> req.labelSelector.let { selector ->
                    if (StringUtils.isNotEmpty(selector.name)) {
                        val values = selector.valueList
                                .map { it?.trim() ?: "" }
                                .filter { StringUtils.isNotEmpty(it) }
                        if (values.isEmpty()) {
                            ExistsMatcher(selector.name)
                        } else {
                            LabelMatcher(selector.name, selector.valueList)
                        }
                    } else {
                        AnyLabelMatcher()
                    }
                }
                req.hasAndSelector() -> AndMatcher(Collections.unmodifiableCollection(req.andSelector.selectorsList.map { convertToMatcher(it) }))
                req.hasOrSelector() -> OrMatcher(Collections.unmodifiableCollection(req.orSelector.selectorsList.map { convertToMatcher(it) }))
                req.hasNotSelector() -> NotMatcher(convertToMatcher(req.notSelector.selector))
                req.hasExistsSelector() -> ExistsMatcher(req.existsSelector.name)
                else -> AnyLabelMatcher()
            }
        }

        @JvmStatic
        fun extractLabels(matcher: Matcher): LabelSelectorMatcher? {
            if (matcher is LabelSelectorMatcher) {
                return matcher
            }
            if (matcher is MultiMatcher) {
                return matcher.getMatcher(LabelSelectorMatcher::class.java)
            }
            return null
        }

        @JvmStatic
        fun extractMethod(matcher: Matcher): MethodMatcher? {
            if (matcher is MethodMatcher) {
                return matcher
            }
            if (matcher is MultiMatcher) {
                return matcher.getMatcher(MethodMatcher::class.java)
            }
            return null
        }
    }

    class Builder {
        private val matchers = ArrayList<Matcher>()

        fun forMethod(name: String): Builder {
            matchers.add(MethodMatcher(name))
            return this
        }

        fun forLabels(matcher: LabelSelectorMatcher): Builder {
            matchers.add(matcher)
            return this
        }

        fun withMatcher(matcher: Matcher?): Builder {
            matcher?.let {
                matchers.add(it)
            }
            return this
        }

        fun build(): Matcher {
            return MultiMatcher(matchers)
        }
    }

    interface Matcher {
        fun matches(up: Upstream): Boolean
    }

    class MultiMatcher(
            private val matchers: Collection<Matcher>
    ): Matcher {
        override fun matches(up: Upstream): Boolean {
            return matchers.all { it.matches(up) }
        }

        fun <T: Matcher> getMatcher(type: Class<T>): T? {
            return matchers.find { type.isAssignableFrom(it.javaClass) } as T?
        }
    }

    class MethodMatcher(
            val method: String
    ): Matcher {
        override fun matches(up: Upstream): Boolean {
            return up.getMethods().isAllowed(method)
        }
    }

    abstract class LabelSelectorMatcher: Matcher {
        override fun matches(up: Upstream): Boolean {
            return up.getLabels().any(this::matches)
        }

        abstract fun matches(labels: UpstreamsConfig.Labels): Boolean
        abstract fun asProto(): BlockchainOuterClass.Selector?
    }

    class EmptyMatcher: Matcher {
        override fun matches(up: Upstream): Boolean {
            return true
        }
    }

    class AnyLabelMatcher: LabelSelectorMatcher() {

        override fun matches(labels: UpstreamsConfig.Labels): Boolean {
            return true
        }

        override fun asProto(): BlockchainOuterClass.Selector? {
            return null
        }

        override fun matches(up: Upstream): Boolean {
            return true
        }
    }

    class LocalAndMatcher(vararg val matchers: Matcher) : Matcher {

        override fun matches(up: Upstream): Boolean {
            return matchers.all { it.matches(up) }
        }

    }

    class LabelMatcher(val name: String, val values: Collection<String>) : LabelSelectorMatcher() {
        override fun matches(labels: UpstreamsConfig.Labels): Boolean {
            return labels.get(name)?.let { labelValue ->
                values.any { it == labelValue }
            } ?: false
        }

        override fun asProto(): BlockchainOuterClass.Selector {
            return BlockchainOuterClass.Selector.newBuilder().setLabelSelector(
                    BlockchainOuterClass.LabelSelector.newBuilder()
                            .setName(name)
                            .addAllValue(values)
            ).build()
        }
    }

    class OrMatcher(val matchers: Collection<LabelSelectorMatcher>): LabelSelectorMatcher() {
        override fun matches(labels: UpstreamsConfig.Labels): Boolean {
            return matchers.any { matcher -> matcher.matches(labels) }
        }

        override fun asProto(): BlockchainOuterClass.Selector {
            return BlockchainOuterClass.Selector.newBuilder().setOrSelector(
                    BlockchainOuterClass.OrSelector.newBuilder()
                            .addAllSelectors(matchers.map { it.asProto() })
                            .build()
            ).build()
        }
    }

    class AndMatcher(val matchers: Collection<LabelSelectorMatcher>): LabelSelectorMatcher() {
        override fun matches(labels: UpstreamsConfig.Labels): Boolean {
            return matchers.all { matcher -> matcher.matches(labels) }
        }

        override fun asProto(): BlockchainOuterClass.Selector {
            return BlockchainOuterClass.Selector.newBuilder().setAndSelector(
                    BlockchainOuterClass.AndSelector.newBuilder()
                            .addAllSelectors(matchers.map { it.asProto() })
                            .build()
            ).build()
        }
    }

    class NotMatcher(val matcher: LabelSelectorMatcher): LabelSelectorMatcher() {
        override fun matches(labels: UpstreamsConfig.Labels): Boolean {
            return !matcher.matches(labels)
        }

        override fun asProto(): BlockchainOuterClass.Selector {
            return BlockchainOuterClass.Selector.newBuilder().setNotSelector(
                    BlockchainOuterClass.NotSelector.newBuilder()
                            .setSelector(matcher.asProto())
                            .build()
            ).build()
        }
    }

    class ExistsMatcher(val name: String): LabelSelectorMatcher() {
        override fun matches(labels: UpstreamsConfig.Labels): Boolean {
            return labels.containsKey(name)
        }

        override fun asProto(): BlockchainOuterClass.Selector {
            return BlockchainOuterClass.Selector.newBuilder().setExistsSelector(
                    BlockchainOuterClass.ExistsSelector.newBuilder()
                            .setName(name)
                            .build()
            ).build()
        }
    }

    class CapabilityMatcher(val capability: Capability) : Matcher {
        override fun matches(up: Upstream): Boolean {
            return up.getCapabilities().contains(capability)
        }
    }

    class GrpcMatcher() : Matcher {
        override fun matches(up: Upstream): Boolean {
            return up.isGrpc()
        }
    }

    class HeightMatcher(val height: Long): Matcher {
        override fun matches(up: Upstream): Boolean {
            return (up.getHead().getCurrentHeight() ?: 0) >= height
        }

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is HeightMatcher) return false

            if (height != other.height) return false

            return true
        }

        override fun hashCode(): Int {
            return height.hashCode()
        }
    }
}