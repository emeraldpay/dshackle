package testing

import com.fasterxml.jackson.databind.ObjectMapper

class BlocksHandler implements CallHandler {

    ObjectMapper objectMapper
    ResourceResponse resourceResponse

    BlocksHandler(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper
        this.resourceResponse = new ResourceResponse(objectMapper)
    }

    @Override
    Result handle(String method, List<Object> params) {
        if (method == "eth_blockNumber") {
            return Result.ok("0x100001")
        }
        if (method == "eth_getBlockByNumber") {
            String blockId = params[0]
            println("get block $blockId")
            return resourceResponse.respondWith("block-${blockId}.json")
        }
        if (method == "eth_getTransactionByHash") {
            String txId = params[0]
            return resourceResponse.respondWith("tx-${txId}.json")
        }
        return null
    }

}
