package testing

class TestcaseHandler implements CallHandler {

    @Override
    Result handle(String method, List<Object> params) {
        // https://github.com/emeraldpay/dshackle/issues/35
        if (method == "eth_call"
                && params[0].to?.toLowerCase() == "0x542156d51D10Db5acCB99f9Db7e7C91B74E80a2c".toLowerCase()) {
            return Result.error(-32015, "VM execution error.")
        }
        return null
    }
}
