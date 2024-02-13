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
import io.emeraldpay.dshackle.upstream.ethereum.domain.BlockHash;
import io.emeraldpay.dshackle.upstream.ethereum.domain.Wei;

import java.io.IOException;

public class BlockJsonSerializer extends EtherJsonSerializer<BlockJson<?>> {

    private TransactionJsonSerializer transactionJsonSerializer = new TransactionJsonSerializer();

    @Override
    public void serialize(BlockJson<?> value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();
        writeField(gen, "number", value.getNumber());
        writeField(gen, "hash", value.getHash());
        writeField(gen, "timestamp", value.getTimestamp());

        Wei baseFeePerGas = value.getBaseFeePerGas();
        if (baseFeePerGas != null) {
            writeField(gen, "baseFeePerGas", baseFeePerGas);
        }

        gen.writeFieldName("transactions");
        gen.writeStartArray();
        if (value.getTransactions() != null) {
            for (Object tx: value.getTransactions()) {
                if (tx == null) {
                    gen.writeNull();
                } else if (tx instanceof TransactionJson) {
                    transactionJsonSerializer.serialize((TransactionJson) tx, gen, serializers);
                } else if (tx instanceof TransactionRefJson) {
                    gen.writeString(((TransactionRefJson) tx).getHash().toHex());
                }
            }
        }
        gen.writeEndArray();

        writeField(gen, "parentHash", value.getParentHash());
        writeField(gen, "sha3Uncles", value.getSha3Uncles());
        writeField(gen, "miner", value.getMiner());
        writeField(gen, "difficulty", value.getDifficulty());
        writeField(gen, "totalDifficulty", value.getTotalDifficulty());
        if (value.getSize() != null) {
            writeField(gen, "size", value.getSize().intValue());
        }
        writeField(gen, "gasLimit", value.getGasLimit());
        writeField(gen, "gasUsed", value.getGasUsed());
        writeField(gen, "extraData", value.getExtraData());
        writeField(gen, "stateRoot", value.getStateRoot());
        writeField(gen, "receiptsRoot", value.getReceiptsRoot());
        writeField(gen, "transactionsRoot", value.getTransactionsRoot());
        writeField(gen, "logsBloom", value.getLogsBloom());
        writeField(gen, "mixHash", value.getMixHash());
        writeField(gen, "nonce", value.getNonce());
        writeField(gen, "withdrawalsRoot", value.getWithdrawalsRoot());

        gen.writeFieldName("uncles");
        gen.writeStartArray();
        if (value.getUncles() != null) {
            for (BlockHash uncle: value.getUncles()) {
                gen.writeString(uncle.toHex());
            }
        }
        gen.writeEndArray();

        gen.writeEndObject();
    }
}
