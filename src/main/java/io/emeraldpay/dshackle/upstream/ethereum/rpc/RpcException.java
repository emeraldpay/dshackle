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

import java.util.Objects;

/**
 * @author Igor Artamonov
 */
public class RpcException extends RuntimeException {

    private int code;
    private String rpcMessage;
    private Object details;

    public RpcException(int code, String rpcMessage, Object details, Throwable cause) {
        super("RPC Error " + code + ": " + rpcMessage, cause);
        this.code = code;
        this.rpcMessage = rpcMessage;
        this.details = details;
    }

    public RpcException(int code, String rpcMessage, Object details) {
        this(code, rpcMessage, details, null);
    }

    public RpcException(int code, String rpcMessage) {
        this(code, rpcMessage, null);
    }

    public int getCode() {
        return code;
    }

    public String getRpcMessage() {
        return rpcMessage;
    }

    public Object getDetails() {
        return details;
    }

    public RpcResponseError getError() {
        return new RpcResponseError(code, rpcMessage, details);
    }

    public String toString() {
        return "RpcException(" + code + " " + rpcMessage
            + (details == null ? "" : ", " + details.toString())
            + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RpcException that = (RpcException) o;
        return code == that.code &&
            Objects.equals(rpcMessage, that.rpcMessage) &&
            Objects.equals(details, that.details);
    }

    @Override
    public int hashCode() {
        return Objects.hash(code, rpcMessage);
    }
}
