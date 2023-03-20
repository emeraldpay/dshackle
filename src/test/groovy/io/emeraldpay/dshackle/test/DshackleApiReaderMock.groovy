package io.emeraldpay.dshackle.test

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleRequest
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleResponse
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcError
import io.emeraldpay.etherjar.rpc.RpcResponseError
import org.jetbrains.annotations.NotNull
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono

import java.util.concurrent.Callable
import java.util.concurrent.atomic.AtomicInteger

class DshackleApiReaderMock implements Reader<DshackleRequest, DshackleResponse> {

    private static final Logger log = LoggerFactory.getLogger(this)
    List<PredefinedResponse> predefined = []
    private final ObjectMapper objectMapper = Global.objectMapper

    String id = "default"
    AtomicInteger calls = new AtomicInteger(0)

    DshackleApiReaderMock() {
    }

    DshackleApiReaderMock answerOnce(@NotNull String method, List<Object> params, Object result) {
        return answer(method, params, result, 1)
    }

    DshackleApiReaderMock answer(@NotNull String method, List<Object> params, Object result,
                                 Integer limit = null, Throwable exception = null) {
        predefined << new PredefinedResponse(method: method, params: params, result: result, limit: limit, exception: exception)
        return this
    }

    @Override
    Mono<DshackleResponse> read(DshackleRequest request, boolean required = true) {
        Callable<DshackleResponse> call = {
            def predefined = predefined.find { it.isSame(request.method, request.params) }
            byte[] result = null
            JsonRpcError error = null
            calls.incrementAndGet()
            if (predefined != null) {
                if (predefined.exception != null) {
                    predefined.onCalled()
                    predefined.print(request.id)
                    throw predefined.exception
                }
                if (predefined.result instanceof RpcResponseError) {
                    ((RpcResponseError) predefined.result).with { err ->
                        error = new JsonRpcError(err.code, err.message)
                    }
                } else {
//                    ResponseJson json = new ResponseJson<Object, Integer>(id: 1, result: predefined.result)
                    result = objectMapper.writeValueAsBytes(predefined.result)
                }
                predefined.onCalled()
                predefined.print(request.id)
            } else {
                log.error("Method ${request.method} with ${request.params} is not mocked")
                if (!required) {
                    return null
                }
                error = new JsonRpcError(-32601, "Method ${request.method} with ${request.params} is not mocked")
            }
            return new DshackleResponse(request.id, result, error, null)
        } as Callable<DshackleResponse>
        return Mono.fromCallable(call)
    }

    class PredefinedResponse {
        String method
        List params
        Object result
        Integer limit
        Throwable exception

        boolean isSame(String method, List<?> params) {
            if (limit != null) {
                if (limit <= 0) {
                    return false
                }
            }
            if (method != this.method) {
                return false
            }
            if (this.params == null) {
                return true
            }
            return this.params == params
        }

        void onCalled() {
            if (limit != null) {
                limit--
            }
        }

        void print(int id) {
            println "Execute API: $id $method ${params ? params : '_'} >> $result"
        }
    }


}
