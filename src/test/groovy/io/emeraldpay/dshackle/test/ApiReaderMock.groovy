/**
 * Copyright (c) 2019 ETCDEV GmbH
 * Copyright (c) 2020 EmeraldPay, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.emeraldpay.dshackle.test

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.protobuf.ByteString
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.ChainCallError
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.ChainResponse
import io.emeraldpay.dshackle.upstream.ethereum.rpc.RpcResponseError
import io.grpc.stub.StreamObserver
import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufAllocator
import io.netty.buffer.ByteBufInputStream
import io.netty.buffer.Unpooled
import io.netty.handler.codec.http.HttpHeaders
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame
import io.netty.handler.codec.http.websocketx.WebSocketCloseStatus
import io.netty.handler.codec.http.websocketx.WebSocketFrame
import org.jetbrains.annotations.NotNull
import org.reactivestreams.Publisher
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.netty.ByteBufFlux
import reactor.netty.Connection
import reactor.netty.NettyInbound
import reactor.netty.NettyOutbound
import reactor.netty.http.websocket.WebsocketInbound
import reactor.netty.http.websocket.WebsocketOutbound
import reactor.util.annotation.Nullable

import java.time.Duration
import java.util.concurrent.Callable
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.BiFunction
import java.util.function.Consumer
import java.util.function.Predicate

class ApiReaderMock implements Reader<ChainRequest, ChainResponse> {

    private static final Logger log = LoggerFactory.getLogger(this)
    List<PredefinedResponse> predefined = []
    private final ObjectMapper objectMapper = Global.objectMapper

    String id = "default"
    AtomicInteger calls = new AtomicInteger(0)

    ApiReaderMock() {
    }

    ApiReaderMock answerOnce(@NotNull String method, List<Object> params, Object result) {
        return answer(method, params, result, 1)
    }

    ApiReaderMock answer(@NotNull String method, List<Object> params, Object result,
                         Integer limit = null, Throwable exception = null) {
        predefined << new PredefinedResponse(method: method, params: params, result: result, limit: limit, exception: exception)
        return this
    }

    @Override
    Mono<ChainResponse> read(ChainRequest request, boolean required = true) {
        Callable<ChainResponse> call = {
            def predefined = predefined.find { it.isSame(request.method, request.params.list) }
            byte[] result = null
            ChainCallError error = null
            calls.incrementAndGet()
            if (predefined != null) {
                if (predefined.exception != null) {
                    predefined.onCalled()
                    predefined.print(request.id)
                    throw predefined.exception
                }
                if (predefined.result instanceof RpcResponseError) {
                    ((RpcResponseError) predefined.result).with { err ->
                        error = new ChainCallError(err.code, err.message)
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
                error = new ChainCallError(-32601, "Method ${request.method} with ${request.params} is not mocked")
            }
            return new ChainResponse(result, error, ChainResponse.Id.from(request.id), null, null, null)
        } as Callable<ChainResponse>
        return Mono.fromCallable(call)
    }

    def nativeCall(BlockchainOuterClass.NativeCallRequest request, StreamObserver<BlockchainOuterClass.NativeCallReplyItem> responseObserver) {
        request.itemsList.forEach { req ->
            ChainResponse resp = read(new ChainRequest(req.method, objectMapper.readerFor(List).readValue(req.payload.toByteArray())))
                    .block(Duration.ofSeconds(5))
            def proto = BlockchainOuterClass.NativeCallReplyItem.newBuilder()
                    .setId(req.id)
                    .setSucceed(resp.hasResult())
                    .setPayload(ByteString.copyFrom(resp.getResult()))

            resp.error?.with { err ->
                proto.setErrorMessage(err.message)
            }
            responseObserver.onNext(proto.build())
        }
        responseObserver.onCompleted()
    }

    WebsocketApi asWebsocket() {
        return new WebsocketApi(this)
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

    class WebsocketApi {
        private final ApiReaderMock api

        private Sinks.Many<ChainResponse> responses = Sinks
                .many()
                .unicast()
                .onBackpressureBuffer()
        private Sinks.Many<String> jsonResponses = Sinks
                .many()
                .unicast()
                .onBackpressureBuffer()
        private WebsocketOutboundMock outbound
        private WebsocketInboundMock inbound

        WebsocketApi(ApiReaderMock api) {
            this.api = api
            outbound = new WebsocketOutboundMock(api, responses)
            inbound = new WebsocketInboundMock(responses.asFlux(), jsonResponses.asFlux())
        }

        boolean send(String json) {
            jsonResponses.tryEmitNext(json).success
        }

        WebsocketOutbound getOutbound() {
            return outbound
        }

        WebsocketInbound getInbound() {
            return inbound
        }
    }

    class WebsocketInboundMock implements WebsocketInbound {

        private final Flux<ChainResponse> responses
        private final Flux<String> jsonResponses

        WebsocketInboundMock(Flux<ChainResponse> responses, Flux<String> jsonResponses) {
            this.responses = responses
            this.jsonResponses = jsonResponses
        }

        @Override
        String selectedSubprotocol() {
            throw new UnsupportedOperationException()
        }

        @Override
        HttpHeaders headers() {
            throw new UnsupportedOperationException()
        }

        @Override
        Mono<WebSocketCloseStatus> receiveCloseStatus() {
            return Mono.empty()
        }

        @Override
        ByteBufFlux receive() {
            throw new UnsupportedOperationException()
        }

        @Override
        Flux<?> receiveObject() {
            throw new UnsupportedOperationException()
        }

        @Override
        NettyInbound withConnection(Consumer<? super Connection> withConnection) {
            return this
        }

        @Override
        Flux<WebSocketFrame> receiveFrames() {
            return Flux.merge(
                    jsonResponses,
                    responses.map {
                        Global.objectMapper.writeValueAsString(it)
                    })
                    .map {
                        println("WS server->client msg: $it")
                        new TextWebSocketFrame(it)
                    }
                    .doOnError { t ->
                        t.printStackTrace()
                    }
        }
    }

    class WebsocketOutboundMock implements WebsocketOutbound {

        private final ApiReaderMock api
        private final Sinks.Many<ChainResponse> responses

        WebsocketOutboundMock(ApiReaderMock api, Sinks.Many<ChainResponse> responses) {
            this.api = api
            this.responses = responses
        }

        @Override
        String selectedSubprotocol() {
            throw new UnsupportedOperationException()
        }

        @Override
        ByteBufAllocator alloc() {
            throw new UnsupportedOperationException()
        }

        private void handle(Publisher<ByteBuf> dataStream) {
            Flux.from(dataStream)
                    .map { it ->
                        Global.objectMapper.readValue(new ByteBufInputStream(it), ChainRequest)
                    }
                    .flatMap { ChainRequest request ->
                        api.read(request, false)
                    }
                    .doOnNext {
                        def status = responses.tryEmitNext(it)
                        if (status.isFailure()) {
                            println("Failed to send through mock: $status")
                        }
                    }
                    .subscribe()
        }

        @Override
        NettyOutbound send(Publisher<? extends ByteBuf> dataStream) {
            handle(dataStream)
            return this
        }

        @Override
        NettyOutbound send(Publisher<? extends ByteBuf> dataStream, Predicate<ByteBuf> predicate) {
            handle(dataStream)
            return this
        }

        @Override
        NettyOutbound sendObject(Publisher<?> dataStream, Predicate<Object> predicate) {
            def msgs = Flux.from(dataStream)
                    .cast(TextWebSocketFrame)
                    .map {
                        Unpooled.wrappedBuffer(it.text().bytes)
                    }
            handle(msgs)
            return this
        }

        @Override
        NettyOutbound sendObject(Object message) {
            return this
        }

        @Override
        <S> NettyOutbound sendUsing(Callable<? extends S> sourceInput, BiFunction<? super Connection, ? super S, ?> mappedInput, Consumer<? super S> sourceCleanup) {
            return this
        }

        @Override
        NettyOutbound withConnection(Consumer<? super Connection> withConnection) {
            return this
        }

        @Override
        Mono<Void> sendClose() {
            return Mono.fromCallable {
                responses.tryEmitComplete()
            }.then()
        }

        @Override
        Mono<Void> sendClose(int rsv) {
            return Mono.fromCallable {
                responses.tryEmitComplete()
            }.then()
        }

        @Override
        Mono<Void> sendClose(int statusCode, @Nullable String reasonText) {
            return Mono.fromCallable {
                responses.tryEmitComplete()
            }.then()
        }

        @Override
        Mono<Void> sendClose(int rsv, int statusCode, @Nullable String reasonText) {
            return Mono.fromCallable {
                responses.tryEmitComplete()
            }.then()
        }
    }
}
