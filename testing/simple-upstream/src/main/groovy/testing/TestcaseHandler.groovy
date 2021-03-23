package testing

import com.fasterxml.jackson.databind.ObjectMapper

class TestcaseHandler implements CallHandler {

    ObjectMapper objectMapper
    ResourceResponse resourceResponse

    TestcaseHandler(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper
        this.resourceResponse = new ResourceResponse(objectMapper)
    }

    @Override
    Result handle(String method, List<Object> params) {
        // https://github.com/emeraldpay/dshackle/issues/35
        if (method == "eth_call"
                && params[0].to?.toLowerCase() == "0x542156d51D10Db5acCB99f9Db7e7C91B74E80a2c".toLowerCase()) {
            return Result.error(-32015, "VM execution error.")
        }
        // https://github.com/emeraldpay/dshackle/issues/35 (second)
        if (method == "eth_call"
                && params[0].to?.toLowerCase() == "0x8ee2a5aca4f88cb8c757b8593d0734855dcc0eba".toLowerCase()) {
            return Result.error(-32015, "VM execution error.", "revert: SafeMath: division by zero")
        }
        // https://github.com/emeraldpay/dshackle/issues/43
        if (method == "debug_traceTransaction"
                && params[0].toLowerCase() == "0xd949bc0fe1a5d16f4522bc47933554dcc4ada0493ff71ee1973b2410257af9fe".toLowerCase()) {
            return resourceResponse.respondWith("trace-0xd949bc.json")
        }
        // https://github.com/emeraldpay/dshackle/issues/67
        if (method == "eth_call"
                && params[0].to?.toLowerCase() == "0xdAC17F958D2ee523a2206206994597C13D831ec7".toLowerCase()) {
            return Result.error(-32000, "invalid opcode: opcode 0xfe not defined")
        }
        // https://github.com/emeraldpay/dshackle/issues/67, when a custom method configured
        if (method == "test_foo"
                && params[0].to?.toLowerCase() == "0xdAC17F958D2ee523a2206206994597C13D831ec7".toLowerCase()) {
            return Result.error(-32000, "invalid opcode: opcode 0xfe not defined")
        }
        return null
    }
}
