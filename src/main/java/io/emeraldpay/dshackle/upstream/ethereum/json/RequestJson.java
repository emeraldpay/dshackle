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

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

public class RequestJson<T> {

    private String jsonrpc = "2.0";
    private String method;
    @JsonInclude(JsonInclude.Include.ALWAYS)
    private List params;
    @JsonInclude(JsonInclude.Include.ALWAYS)
    private T id;

    public RequestJson(String method, List params, T id) {
        if (!(
            Integer.class.isAssignableFrom(id.getClass())
                || Long.class.isAssignableFrom(id.getClass())
                || String.class.isAssignableFrom(id.getClass())
        )) {
            throw new IllegalArgumentException("ID must be String or Integer/Long");
        }
        this.method = method;
        this.params = params;
        this.id = id;
    }

    public String getJsonrpc() {
        return jsonrpc;
    }

    public String getMethod() {
        return method;
    }

    public List getParams() {
        return params;
    }

    public T getId() {
        return id;
    }
}
