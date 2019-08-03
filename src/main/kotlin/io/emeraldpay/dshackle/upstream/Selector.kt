package io.emeraldpay.dshackle.upstream

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.dshackle.config.UpstreamsConfig
import org.apache.commons.lang3.StringUtils
import java.util.*

class Selector {

    companion object {

        val empty = EmptyMatcher()

        @JvmStatic
        fun convertToMatcher(req: BlockchainOuterClass.Selector?): Matcher {
            return when {
                req == null -> EmptyMatcher()
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
                        EmptyMatcher()
                    }
                }
                req.hasAndSelector() -> AndMatcher(Collections.unmodifiableCollection(req.andSelector.selectorsList.map { convertToMatcher(it) }))
                req.hasOrSelector() -> OrMatcher(Collections.unmodifiableCollection(req.orSelector.selectorsList.map { convertToMatcher(it) }))
                req.hasNotSelector() -> NotMatcher(convertToMatcher(req.notSelector.selector))
                req.hasExistsSelector() -> ExistsMatcher(req.existsSelector.name)
                else -> EmptyMatcher()
            }
        }
    }

    interface Matcher {
        fun matches(labels: UpstreamsConfig.Labels): Boolean
        fun asProto(): BlockchainOuterClass.Selector?
    }

    class EmptyMatcher: Matcher {
        override fun matches(labels: UpstreamsConfig.Labels): Boolean {
            return true
        }

        override fun asProto(): BlockchainOuterClass.Selector? {
            return null
        }
    }

    class LabelMatcher(val name: String, val values: Collection<String>): Matcher {
        override fun matches(labels: UpstreamsConfig.Labels): Boolean {
            return labels.get(name)?.let {
                labelValue -> values.any { it == labelValue }
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

    class OrMatcher(val matchers: Collection<Matcher>): Matcher {
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

    class AndMatcher(val matchers: Collection<Matcher>): Matcher {
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

    class NotMatcher(val matcher: Matcher): Matcher {
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

    class ExistsMatcher(val name: String): Matcher {
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

}