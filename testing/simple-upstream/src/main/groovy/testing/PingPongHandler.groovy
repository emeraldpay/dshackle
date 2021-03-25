package testing

class PingPongHandler implements CallHandler {
    @Override
    Result handle(String method, List<Object> params) {
        if (method == "eth_call"
                && params[0].to?.toLowerCase() == "0x0123456789abcdef0123456789abcdef00000001".toLowerCase()) {
            return Result.ok([data: params[0].data])
        }
        return null
    }
}
