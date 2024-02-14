/*
 * Copyright (c) 2021 EmeraldPay Inc, All Rights Reserved.
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
import com.fasterxml.jackson.databind.SerializerProvider;
import io.emeraldpay.dshackle.upstream.ethereum.hex.HexData;

import java.io.IOException;

public class TransactionLogJsonSerializer extends EtherJsonSerializer<TransactionLogJson> {


    @Override
    public void serialize(TransactionLogJson value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();
        writeField(gen, "address", value.getAddress());
        writeField(gen, "blockHash", value.getBlockHash());
        writeField(gen, "blockNumber", value.getBlockNumber());
        writeField(gen, "data", value.getData());
        writeField(gen, "logIndex", value.getLogIndex());

        gen.writeFieldName("topics");
        gen.writeStartArray();
        if (value.getTopics() != null) {
            for (HexData topic : value.getTopics()) {
                gen.writeString(topic.toString());
            }
        }
        gen.writeEndArray();

        writeField(gen, "transactionHash", value.getHash());
        writeField(gen, "transactionIndex", value.getTransactionIndex());
        writeField(gen, "removed", value.getRemoved());
        gen.writeEndObject();
    }
}
