package io.emeraldpay.dshackle.upstream.calls

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import io.emeraldpay.dshackle.quorum.CallQuorum
import java.time.Duration

class DisabledCallMethods(
    defaultDisableTimeout: Long,
    private val callMethods: CallMethods,
    private val onUpdate: () -> Unit,
) : CallMethods {
    var disabledMethods: Cache<String, Boolean> = Caffeine.newBuilder()
        .removalListener { key: String?, _: Boolean?, cause ->
            if (cause.wasEvicted() && key != null) {
                onUpdate.invoke()
            }
        }
        .expireAfterWrite(Duration.ofMinutes(defaultDisableTimeout))
        .build<String, Boolean>()

    constructor(
        onUpdate: () -> Unit,
        defaultDisableTimeout: Long,
        callMethods: CallMethods,
        disabledMethodsCopy: Cache<String, Boolean>,
    ) : this(defaultDisableTimeout, callMethods, onUpdate) {
        disabledMethods = disabledMethodsCopy
    }

    override fun createQuorumFor(method: String): CallQuorum {
        return callMethods.createQuorumFor(method)
    }

    override fun isCallable(method: String): Boolean {
        return callMethods.isCallable(method) && disabledMethods.getIfPresent(method) == null
    }

    override fun getSupportedMethods(): Set<String> {
        return callMethods.getSupportedMethods() - disabledMethods.asMap().keys
    }

    override fun isHardcoded(method: String): Boolean {
        return callMethods.isHardcoded(method)
    }

    override fun executeHardcoded(method: String): ByteArray {
        return callMethods.executeHardcoded(method)
    }

    override fun getGroupMethods(groupName: String): Set<String> {
        return callMethods.getGroupMethods(groupName)
    }

    fun disableMethodTemporarily(method: String) {
        disabledMethods.put(method, true)
    }
}
