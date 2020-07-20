package testing

import com.fasterxml.jackson.databind.ObjectMapper

class BlocksHandler implements CallHandler {

    ObjectMapper objectMapper

    BlocksHandler(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper
    }

    @Override
    Result handle(String method, List<Object> params) {
        if (method == "eth_blockNumber") {
            return Result.ok("0x100001")
        }
        if (method == "eth_getBlockByNumber") {
            String blockId = params[0]
            println("get block $blockId")
            def result = getResource("block-${blockId}.json")
            return Result.ok(result)
        }
        if (method == "eth_getTransactionByHash") {
            String txId = params[0]
            def result = getResource("tx-${txId}.json")
            return Result.ok(result)
        }
        return null
    }

    Object getResource(String name) {
        String json = BlocksHandler.class.getResourceAsStream("/" + name)?.text
        if (json == null) {
            return null
        }
        return objectMapper.readValue(json, Map)
    }
}
