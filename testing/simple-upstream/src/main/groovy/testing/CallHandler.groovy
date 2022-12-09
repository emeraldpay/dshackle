package testing

interface CallHandler {

    Result handle(String method, List<Object> params)

    static class Result {
        private Object result
        private Integer errorCode
        private String errorMessage
        private Object errorDetails

        Result(Object result, Integer errorCode, String errorMessage, Object errorDetails) {
            this.result = result
            this.errorCode = errorCode
            this.errorMessage = errorMessage
            this.errorDetails = errorDetails
        }

        static Result ok(Object result) {
            return new Result(result, null, null, null)
        }

        static Result error(int errorCode, String errorMessage, Object details = null) {
            return new Result(null, errorCode, errorMessage, details)
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

        Object getErrorDetails() {
            return errorDetails
        }
    }
}