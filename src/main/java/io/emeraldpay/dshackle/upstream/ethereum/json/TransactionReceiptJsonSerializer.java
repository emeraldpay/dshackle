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
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

public class TransactionReceiptJsonSerializer extends EtherJsonSerializer<TransactionReceiptJson> {

    @Override
    public void serialize(TransactionReceiptJson value, JsonGenerator gen, SerializerProvider serializers) throws IOException {

        gen.writeStartObject();
        writeField(gen, "blockHash", value.getBlockHash());
        writeField(gen, "blockNumber", value.getBlockNumber());
        writeField(gen, "contractAddress", value.getContractAddress());
        writeField(gen, "from", value.getFrom());
        writeField(gen, "to", value.getTo());
        writeField(gen, "cumulativeGasUsed", value.getCumulativeGasUsed());
        writeField(gen, "gasUsed", value.getGasUsed());
        writeField(gen, "effectiveGasPrice", value.getEffectiveGasPrice());
        writeField(gen, "transactionHash", value.getTransactionHash());
        writeField(gen, "transactionIndex", value.getTransactionIndex());
        writeField(gen, "logsBloom", value.getLogsBloom());

        gen.writeFieldName("logs");
        gen.writeStartArray();
        if (value.getLogs() != null) {
            JsonSerializer<Object> transactionLogJsonSerialized = serializers.findValueSerializer(TransactionLogJson.class);
            for (TransactionLogJson logJson : value.getLogs()) {
                transactionLogJsonSerialized.serialize(logJson, gen, serializers);
            }
        }
        gen.writeEndArray();

        if (value.getStatus() != null) {
            writeField(gen, "status", value.getStatus());
        }
        if (value.getType() != 0) {
            writeField(gen, "type", value.getType());
        }
        gen.writeEndObject();
    }
}
