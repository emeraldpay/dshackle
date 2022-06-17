package testing

import com.fasterxml.jackson.databind.ObjectMapper

class CommonHandlers implements CallHandler {

    ObjectMapper objectMapper
    ResourceResponse resourceResponse

    CommonHandlers(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper
        this.resourceResponse = new ResourceResponse(objectMapper)
    }

    @Override
    Result handle(String method, List<Object> params) {
        if (method == "eth_syncing") {
            return Result.ok(false)
        }
        if (method == "eth_chainId") {
            return resourceResponse.respondWith("chain-id.json")
        }
        return null
    }
}
