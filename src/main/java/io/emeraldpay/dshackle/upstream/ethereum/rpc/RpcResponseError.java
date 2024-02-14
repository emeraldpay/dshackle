/*
 * Copyright (c) 2020 EmeraldPay Inc, All Rights Reserved.
 * Copyright (c) 2016-2017 Infinitape Inc, All Rights Reserved.
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
package io.emeraldpay.dshackle.upstream.ethereum.rpc;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * @author Igor Artamonov
 */
public class RpcResponseError {

    public static int CODE_INVALID_JSON = -32700;
    public static int CODE_INVALID_REQUEST = -32600;
    public static int CODE_METHOD_NOT_EXIST = -32601;
    public static int CODE_INVALID_METHOD_PARAMS = -32602;
    public static int CODE_INTERNAL_ERROR = -32603;

    public static int CODE_RESERVED_0 = -32000;
    public static int CODE_RESERVED_99 = -32099;

    public static int CODE_UPSTREAM_INVALID_RESPONSE = -32000;
    public static int CODE_UPSTREAM_CONNECTION_ERROR = -32001;

    /**
     * -32700 - Invalid JSON was received by the server. An error occurred on the server while parsing the JSON text.
     * -32600 - The JSON sent is not a valid Request object.
     * -32601 - The method does not exist / is not available.
     * -32602 - Invalid method parameter(s).
     * -32603 - Internal JSON-RPC error.
     * -32000 to -32099 - Reserved for implementation-defined server-errors.
     */
    private int code;

    /**
     * A String providing a short description of the error.
     * The message SHOULD be limited to a concise single sentence.
     */
    private String message;


    /**
     * A Primitive or Structured value that contains additional information about the error.
     * This may be omitted.
     * The value of this member is defined by the Server (e.g. detailed error information, nested errors etc.).
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Object data;

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    public Object getData() {
        return data;
    }

    public RpcResponseError() {
    }

    public RpcResponseError(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public RpcResponseError(int code, String message, Object data) {
        this.code = code;
        this.message = message;
        this.data = data;
    }

    /**
     * Convert it ot RpcException
     *
     * @return same error as exception
     * @see RpcException
     */
    public RpcException asException() {
        return new RpcException(code, message, data);
    }

    public String toString() {
        return "RPC Response Error (code: " + code + "; message: " + message + ")";
    }
}
