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
        // https://github.com/emeraldpay/dshackle/issues/43
        if (method == "debug_traceTransaction"
                && params[0].toLowerCase() == "0xd949bc0fe1a5d16f4522bc47933554dcc4ada0493ff71ee1973b2410257af9fe".toLowerCase()) {
            return resourceResponse.respondWith("trace-0xd949bc.json")
        }
        return null
    }
}
