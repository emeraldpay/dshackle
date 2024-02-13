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
import io.emeraldpay.etherjar.hex.Hex32;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TransactionLogJsonDeserializer extends EtherJsonDeserializer<TransactionLogJson> {

    @Override
    public TransactionLogJson deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        JsonNode node = jp.readValueAsTree();
        return deserialize(node);
    }

    public TransactionLogJson deserialize(JsonNode node) {
        TransactionLogJson log = new TransactionLogJson();

        log.setAddress(getAddress(node, "address"));
        log.setBlockHash(getBlockHash(node, "blockHash"));
        log.setBlockNumber(getLong(node, "blockNumber"));
        log.setData(getData(node, "data"));
        log.setLogIndex(getLong(node, "logIndex"));
        List<Hex32> topics = new ArrayList<>();
        for (JsonNode topic: node.get("topics")) {
            topics.add(Hex32.from(topic.textValue()));
        }
        log.setTopics(topics);
        log.setTransactionHash(getTxHash(node, "transactionHash"));
        log.setTransactionIndex(getLong(node, "transactionIndex"));
        log.setRemoved(getBoolean(node, "removed"));

        return log;
    }
}
