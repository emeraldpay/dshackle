package testing

class CommonHandlers implements CallHandler {


    @Override
    Result handle(String method, List<Object> params) {
        if (method == "eth_syncing") {
            return Result.ok(false)
        }
        return null
    }
}
