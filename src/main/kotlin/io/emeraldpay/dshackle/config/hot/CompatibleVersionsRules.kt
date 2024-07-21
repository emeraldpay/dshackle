package io.emeraldpay.dshackle.config.hot

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty

@JsonIgnoreProperties(ignoreUnknown = true)
data class CompatibleVersionsRules(
    @JsonProperty("rules")
    val rules: List<CompatibleVersionsRule>,
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class CompatibleVersionsRule(
    @JsonProperty("client")
    val client: String,
    @JsonProperty("blacklist")
    val blacklist: List<String>?,
    @JsonProperty("whitelist")
    val whitelist: List<String>?,
)
