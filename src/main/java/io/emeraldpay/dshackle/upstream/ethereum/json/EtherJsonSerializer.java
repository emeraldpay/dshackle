/*
 * Copyright (c) 2016-2019 Igor Artamonov, All Rights Reserved.
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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import io.emeraldpay.dshackle.upstream.ethereum.domain.Wei;
import io.emeraldpay.dshackle.upstream.ethereum.hex.HexData;

import java.io.IOException;
import java.math.BigInteger;
import java.time.Instant;

public abstract class EtherJsonSerializer<T> extends JsonSerializer<T> {

    protected void writeField(JsonGenerator gen, String name, HexData value) throws IOException {
        if (value == null) {
            return;
        }
        gen.writeStringField(name, value.toHex());
    }

    protected void writeField(JsonGenerator gen, String name, Wei value) throws IOException {
        if (value == null) {
            return;
        }
        gen.writeStringField(name, value.toHex());
    }

    protected void writeField(JsonGenerator gen, String name, BigInteger value) throws IOException {
        if (value == null) {
            return;
        }
        gen.writeStringField(name, "0x"+value.toString(16));
    }

    protected void writeField(JsonGenerator gen, String name, Integer value) throws IOException {
        if (value == null) {
            return;
        }
        gen.writeStringField(name, "0x"+ Integer.toString(value,16));
    }

    protected void writeField(JsonGenerator gen, String name, Long value) throws IOException {
        if (value == null) {
            return;
        }
        gen.writeStringField(name, "0x"+ Long.toString(value,16));
    }

    protected void writeFieldNumber(JsonGenerator gen, String name, Integer value) throws IOException {
        if (value == null) {
            return;
        }
        gen.writeNumberField(name, value);
    }

    protected void writeField(JsonGenerator gen, String name, Instant value) throws IOException {
        if (value == null) {
            return;
        }
        gen.writeStringField(name, "0x"+ Long.toString(value.toEpochMilli() / 1000L, 16));
    }

    protected void writeField(JsonGenerator gen, String name, Boolean value) throws IOException {
        if (value == null) {
            return;
        }
        gen.writeBooleanField(name, value);
    }
}
