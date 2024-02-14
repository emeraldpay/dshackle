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

package io.emeraldpay.dshackle.upstream.ethereum.json;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.emeraldpay.dshackle.upstream.ethereum.rpc.RpcResponseError;

@JsonSerialize(using = ResponseJsonSerializer.class)
public class ResponseJson<DATA, ID> {

    private String jsonrpc = "2.0";
    private ID id;
    private DATA result;
    private RpcResponseError error;

    public String getJsonrpc() {
        return jsonrpc;
    }

    public void setJsonrpc(String jsonrpc) {
        this.jsonrpc = jsonrpc;
    }

    public ID getId() {
        return id;
    }

    public void setId(ID id) {
        if (!(
            Integer.class.isAssignableFrom(id.getClass())
                || Long.class.isAssignableFrom(id.getClass())
                || String.class.isAssignableFrom(id.getClass())
        )) {
            throw new IllegalArgumentException("ID must be String or Integer/Long");
        }
        this.id = id;
    }

    public DATA getResult() {
        return result;
    }

    public void setResult(DATA result) {
        this.result = result;
    }

    public RpcResponseError getError() {
        return error;
    }

    public void setError(RpcResponseError error) {
        this.error = error;
    }

    @SuppressWarnings("unchecked")
    public <T> ResponseJson<T, ID> cast(Class<T> clazz) {
        if (result == null || clazz.isAssignableFrom(result.getClass())) {
            return (ResponseJson<T, ID>) this;
        }
        throw new ClassCastException("Value of " + result.getClass() + " is not assignable to " + clazz);
    }
}
