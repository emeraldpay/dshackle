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
import io.emeraldpay.dshackle.upstream.ethereum.domain.Address;
import io.emeraldpay.dshackle.upstream.ethereum.domain.ChainId;
import io.emeraldpay.dshackle.upstream.ethereum.domain.TransactionSignature;
import io.emeraldpay.dshackle.upstream.ethereum.hex.Hex32;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TransactionJsonDeserializer extends EtherJsonDeserializer<TransactionJson> {

    @Override
    public TransactionJson deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        JsonNode node = jp.readValueAsTree();
        return deserialize(node);
    }

    public TransactionJson deserialize(JsonNode node) {
        TransactionJson tx = new TransactionJson();
        tx.setHash(getTxHash(node, "hash"));
        tx.setNonce(getLong(node, "nonce"));
        tx.setBlockHash(getBlockHash(node, "blockHash"));
        Integer type = getInt(node, "type");
        if (type != null) {
            tx.setType(type);
        }
        Integer chainId = getInt(node, "chainId");
        if (chainId != null) {
            tx.setChainId(chainId);
        }
        Long blockNumber = getLong(node, "blockNumber");
        if (blockNumber != null)  {
            tx.setBlockNumber(blockNumber);
        }
        Long txIndex = getLong(node, "transactionIndex");
        if (txIndex != null) {
            tx.setTransactionIndex(txIndex);
        }
        tx.setFrom(getAddress(node, "from"));
        tx.setTo(getAddress(node, "to"));
        tx.setValue(getWei(node, "value"));
        tx.setGasPrice(getWei(node, "gasPrice"));
        tx.setMaxFeePerGas(getWei(node, "maxFeePerGas"));
        tx.setMaxPriorityFeePerGas(getWei(node, "maxPriorityFeePerGas"));
        tx.setGas(getLong(node, "gas"));
        tx.setInput(getData(node, "input"));

        if (node.has("r") && node.has("v") && node.has("s")) {
            TransactionSignature signature = new TransactionSignature();

            if (node.hasNonNull("networkId")) {
                signature.setChainId(new ChainId(node.get("networkId").intValue()));
            }
            signature.setR(getData(node, "r"));
            signature.setS(getData(node, "s"));
            signature.setV(getLong(node, "v"));
            signature.setPublicKey(getData(node, "publicKey"));

            tx.setSignature(signature);
        }

        if (node.has("accessList")) {
            List<TransactionJson.Access> accessList = new ArrayList<>();
            for (JsonNode access: node.get("accessList")) {
                Address address = getAddress(access, "address");
                if (access.has("storageKeys")) {
                    List<Hex32> storageKeys = new ArrayList<>();
                    for (JsonNode storageKey: access.get("storageKeys")) {
                        storageKeys.add(Hex32.from(storageKey.textValue()));
                    }
                    accessList.add(new TransactionJson.Access(address, storageKeys));
                } else {
                    accessList.add(new TransactionJson.Access(address));
                }
            }
            tx.setAccessList(accessList);
        }

        return tx;
    }
}
