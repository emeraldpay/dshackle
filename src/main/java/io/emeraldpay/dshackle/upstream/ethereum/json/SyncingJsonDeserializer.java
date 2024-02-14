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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;

public class SyncingJsonDeserializer extends EtherJsonDeserializer<SyncingJson> {

    @Override
    public SyncingJson deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        JsonNode node = jp.readValueAsTree();
        SyncingJson resp = new SyncingJson();
        if (node.isBoolean()) {
            resp.setSyncing(node.asBoolean());
        } else {
            resp.setSyncing(true);
            resp.setStartingBlock(getLong(node, "startingBlock"));
            resp.setCurrentBlock(getLong(node, "currentBlock"));
            resp.setHighestBlock(getLong(node, "highestBlock"));
        }
        return resp;
    }
}
