package io.emeraldpay.dshackle.config

/**
 * Update configuration value from environment variables. Format: ${ENV_VAR_NAME}
 */
class EnvVariables {

    companion object {
        private val envRegex = Regex("\\$\\{(\\w+?)}")
    }

    fun postProcess(value: String): String {
        return envRegex.replace(value) { m ->
            m.groups[1]?.let { g ->
                System.getProperty(g.value) ?: System.getenv(g.value) ?: ""
            } ?: ""
        }
    }
}