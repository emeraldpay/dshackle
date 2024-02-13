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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import io.emeraldpay.etherjar.hex.HexEncoding;

import java.io.IOException;
import java.math.BigInteger;

public class TransactionCallJsonSerializer extends JsonSerializer<TransactionCallJson> {

    @Override
    public void serialize(TransactionCallJson value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException, JsonProcessingException
    {
        if (value == null) {
            gen.writeNull();
            return;
        }
        gen.writeStartObject();
        if (value.getFrom() != null) {
            gen.writeFieldName("from");
            gen.writeString(value.getFrom().toHex());
        }
        if (value.getTo() != null) {
            gen.writeFieldName("to");
            gen.writeString(value.getTo().toHex());
        }
        if (value.getGas() != null) {
            gen.writeFieldName("gas");
            gen.writeString(HexEncoding.toHex(BigInteger.valueOf(value.getGas())));
        }
        if (value.getGasPrice() != null) {
            gen.writeFieldName("gasPrice");
            gen.writeString(HexEncoding.toHex(value.getGasPrice().getAmount()));
        }
        if (value.getValue() != null) {
            gen.writeFieldName("value");
            gen.writeString(HexEncoding.toHex(value.getValue().getAmount()));
        }
        if (value.getData() != null) {
            gen.writeFieldName("data");
            gen.writeString(value.getData().toHex());
        }
        if (value.getNonce() != null) {
            gen.writeFieldName("nonce");
            gen.writeString(HexEncoding.toHex(value.getNonce()));
        }
        gen.writeEndObject();
    }
}
