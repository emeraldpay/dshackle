package testing

class InvalidCallHandler implements CallHandler {
    @Override
    Result handle(String method, List<Object> params) {
        return Result.error(-32000, "Call is not supported")
    }
}
