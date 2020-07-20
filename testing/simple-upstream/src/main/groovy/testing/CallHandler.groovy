package testing

interface CallHandler {

    Result handle(String method, List<Object> params)

    static class Result {
        private Object result
        private Integer errorCode
        private String errorMessage

        Result(Object result, Integer errorCode, String errorMessage) {
            this.result = result
            this.errorCode = errorCode
            this.errorMessage = errorMessage
        }

        static Result ok(Object result) {
            return new Result(result, null, null)
        }

        static Result error(int errorCode, String errorMessage) {
            return new Result(null, errorCode, errorMessage)
        }

        boolean isResult() {
            return errorCode == null
        }

        Object getResult() {
            return result
        }

        int getErrorCode() {
            return errorCode
        }

        String getErrorMessage() {
            return errorMessage
        }
    }
}