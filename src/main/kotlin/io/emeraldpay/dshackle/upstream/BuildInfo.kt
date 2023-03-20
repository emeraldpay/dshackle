package io.emeraldpay.dshackle.upstream

import io.emeraldpay.api.proto.BlockchainOuterClass

data class BuildInfo(var version: String? = null) {
    fun update(buildInfo: BuildInfo): Boolean {
        val changed = buildInfo.version != version
        version = buildInfo.version
        return changed
    }

    companion object {
        fun extract(buildInfo: BlockchainOuterClass.BuildInfo): BuildInfo {
            return BuildInfo(buildInfo.version)
        }
    }
}
