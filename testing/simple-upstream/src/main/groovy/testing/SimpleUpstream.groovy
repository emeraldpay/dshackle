package testing

import com.fasterxml.jackson.databind.ObjectMapper
import spark.Spark

class SimpleUpstream {

    private int port = System.getenv("DSHACKLE_TESTUP_PORT")?.toInteger() ?: 18545
    private ObjectMapper objectMapper

    private List<CallHandler> handlers = []
    private InternalHandler internalHandler

    void prepare() {
        objectMapper = new ObjectMapper()

        internalHandler = new InternalHandler()

        handlers << new TestcaseHandler(objectMapper)
        handlers << new CommonHandlers(objectMapper)
        handlers << new BlocksHandler(objectMapper)
        handlers << new PingPongHandler()
        handlers << internalHandler

        handlers << new InvalidCallHandler()
    }

    void start() {
        println("Starting upstream on 0.0.0.0:$port")
        Spark.port(port)
        Spark.post("/") { req, resp ->
            try {
                def requestBody = req.body()
                println("request: $requestBody")
                internalHandler.record(requestBody)
                Map json = objectMapper.readValue(requestBody, Map)
                def id = json["id"]
                String method = json["method"].toString()
                List<Object> params = json.containsKey("params") ? json["params"] as List<Object> : []

                CallHandler.Result result = handlers.findResult { h ->
                    return h.handle(method, params)
                }
                Map resultJson = [
                        id     : id,
                        jsonrpc: "2.0"
                ]
                if (result.isResult()) {
                    resultJson["result"] = result.result
                } else {
                    resultJson["error"] = [
                            code   : result.getErrorCode(),
                            message: result.getErrorMessage()
                    ]
                    if (result.getErrorDetails() != null) {
                        resultJson["error"]["data"] = result.getErrorDetails()
                    }
                }
                resp.status(200)
                resp.header("content-type", "application/json")
                return objectMapper.writeValueAsString(resultJson)
            } catch (Throwable t) {
                t.printStackTrace()
            }
        }
    }

    public static void main(String[] args) {
        def server = new SimpleUpstream()
        server.prepare()
        server.start()
    }
}
