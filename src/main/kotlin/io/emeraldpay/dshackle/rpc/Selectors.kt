package io.emeraldpay.dshackle.rpc

import io.emeraldpay.api.proto.BlockchainOuterClass.*

object Selectors {

    private fun processMultiple(original: List<Selector>, creator: (List<Selector>) -> Selector.Builder): Selector? {
        val newSelectors = original.mapNotNull { keepForwarded(it) }
        return if (newSelectors.size > 1) {
            return creator(newSelectors).setShouldBeForwarded(true).build()
        } else if (newSelectors.size == 1) {
            newSelectors.first()
        } else {
            null
        }
    }

    fun keepForwarded(selector: Selector): Selector? {
        if (!selector.shouldBeForwarded) return null

        if (selector.hasOrSelector()) {
            return processMultiple(selector.orSelector.selectorsList) {
                Selector.newBuilder()
                    .setOrSelector(OrSelector.newBuilder().addAllSelectors(it))
            }
        } else if (selector.hasAndSelector()) {
            return processMultiple(selector.andSelector.selectorsList) {
                Selector.newBuilder()
                    .setAndSelector(AndSelector.newBuilder().addAllSelectors(it))
            }
        } else if (selector.hasNotSelector()) {
            val inner = keepForwarded(selector.notSelector.selector)
            return inner?.let {
                Selector.newBuilder()
                    .setNotSelector(NotSelector.newBuilder().setSelector(it).build())
                    .setShouldBeForwarded(true)
                    .build()
            }
        } else {
            return selector
        }
    }
}
