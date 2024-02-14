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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import io.emeraldpay.dshackle.upstream.ethereum.domain.BlockHash;
import io.emeraldpay.dshackle.upstream.ethereum.domain.Bloom;
import io.emeraldpay.dshackle.upstream.ethereum.domain.TransactionId;
import io.emeraldpay.dshackle.upstream.ethereum.hex.HexData;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class BlockJsonDeserializer extends EtherJsonDeserializer<BlockJson<?>> {

    private final TransactionJsonDeserializer transactionJsonDeserializer = new TransactionJsonDeserializer();

    @Override @SuppressWarnings("unchecked")
    public BlockJson deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        JsonNode node = jp.readValueAsTree();
        return deserialize(node);
    }

    public BlockJson<? extends TransactionRefJson> deserialize(JsonNode node) {
        BlockJson<TransactionRefJson> blockJson = new BlockJson<>();
        Long number = getLong(node, "number");
        if (number != null) {
            blockJson.setNumber(number);
        }
        blockJson.setHash(getBlockHash(node, "hash"));
        Long timestamp = getLong(node, "timestamp");
        if (timestamp != null) {
            blockJson.setTimestamp(Instant.ofEpochSecond(timestamp));
        } else {
            System.err.println("Null timestamp for block " + number);
        }

        if (node.has("transactions")) {
            List<TransactionRefJson> txes = new ArrayList<>();
            for (JsonNode tx: node.get("transactions")) {
                if (tx.isObject()) {
                    txes.add(transactionJsonDeserializer.deserialize(tx));
                } else {
                    txes.add(new TransactionRefJson(TransactionId.from(tx.textValue())));
                }
            }
            blockJson.setTransactions(txes);
        }

        blockJson.setParentHash(getBlockHash(node, "parentHash"));
        blockJson.setSha3Uncles(getData(node, "sha3Uncles"));
        blockJson.setMiner(getAddress(node, "miner"));
        blockJson.setDifficulty(getQuantity(node, "difficulty"));
        blockJson.setTotalDifficulty(getQuantity(node, "totalDifficulty"));
        Long size = getLong(node, "size");
        if (size != null) {
            blockJson.setSize(size);
        }
        blockJson.setGasLimit(getLong(node, "gasLimit"));
        blockJson.setGasUsed(getLong(node, "gasUsed"));
        blockJson.setExtraData(getData(node, "extraData"));
        HexData logsBloom = getData(node, "logsBloom");
        if (logsBloom != null) {
            blockJson.setLogsBloom(Bloom.from(logsBloom));
        }

        List<BlockHash> uncles = new ArrayList<>();
        JsonNode unclesNode = node.get("uncles");
        if (unclesNode != null && unclesNode.isArray()) {
            for (JsonNode tx: unclesNode) {
                uncles.add(BlockHash.from(tx.textValue()));
            }
        }
        blockJson.setUncles(uncles);

        blockJson.setBaseFeePerGas(getWei(node, "baseFeePerGas"));

        blockJson.setReceiptsRoot(getData(node, "receiptsRoot"));
        blockJson.setStateRoot(getData(node, "stateRoot"));
        blockJson.setTransactionsRoot(getData(node, "transactionsRoot"));
        blockJson.setNonce(getData(node, "nonce"));
        blockJson.setMixHash(getData(node, "mixHash"));
        blockJson.setWithdrawalsRoot(getData(node, "withdrawalsRoot"));

        return blockJson;
    }

}
