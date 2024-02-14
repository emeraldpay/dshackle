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
import com.fasterxml.jackson.databind.SerializerProvider;
import io.emeraldpay.dshackle.upstream.ethereum.domain.TransactionSignature;
import io.emeraldpay.dshackle.upstream.ethereum.hex.Hex32;

import java.io.IOException;
import java.util.List;

public class TransactionJsonSerializer extends EtherJsonSerializer<TransactionJson> {
    @Override
    public void serialize(TransactionJson value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();
        writeField(gen, "hash", value.getHash());
        writeField(gen, "nonce", value.getNonce());
        writeField(gen, "blockHash", value.getBlockHash());
        writeField(gen, "blockNumber", value.getBlockNumber());
        if (value.getType() != 0) {
            writeField(gen, "type", value.getType());
        }
        writeField(gen, "chainId", value.getChainId());
        writeField(gen, "maxFeePerGas", value.getMaxFeePerGas());
        writeField(gen, "maxPriorityFeePerGas", value.getMaxPriorityFeePerGas());
        if (value.getTransactionIndex() != null) {
            writeField(gen, "transactionIndex", value.getTransactionIndex().intValue());
        }
        writeField(gen, "from", value.getFrom());
        if (value.getTo() == null) {
            gen.writeNullField("to");
        } else {
            writeField(gen, "to", value.getTo());
        }
        writeField(gen, "creates", value.getCreates());
        writeField(gen, "value", value.getValue());
        writeField(gen, "gasPrice", value.getGasPrice());
        writeField(gen, "gas", value.getGas());
        writeField(gen, "input", value.getInput());
        TransactionSignature signature = value.getSignature();
        if (signature != null) {
            if (signature.getChainId() != null) {
                writeField(gen, "networkId", signature.getChainId().getValue());
            }
            writeField(gen, "r", signature.getR());
            writeField(gen, "s", signature.getS());
            if (signature.getV() != null) {
                writeField(gen, "v", signature.getV().longValue());
            }
            writeField(gen, "publicKey", signature.getPublicKey());
        }
        List<TransactionJson.Access> accessList = value.getAccessList();
        if (accessList != null) {
            gen.writeFieldName("accessList");
            gen.writeStartArray();
            for (TransactionJson.Access access: accessList) {
                gen.writeStartObject();
                writeField(gen, "address", access.getAddress());
                gen.writeFieldName("storageKeys");
                gen.writeStartArray();
                List<Hex32> storageKeys = access.getStorageKeys();
                if (storageKeys != null) {
                    for (Hex32 key : storageKeys) {
                        gen.writeString(key.toHex());
                    }
                }
                gen.writeEndArray();
                gen.writeEndObject();
            }
            gen.writeEndArray();
        }
        gen.writeEndObject();
    }
}
