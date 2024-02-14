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

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import io.emeraldpay.dshackle.upstream.ethereum.domain.Address;
import io.emeraldpay.dshackle.upstream.ethereum.domain.BlockHash;
import io.emeraldpay.dshackle.upstream.ethereum.domain.TransactionId;
import io.emeraldpay.dshackle.upstream.ethereum.domain.Wei;
import io.emeraldpay.dshackle.upstream.ethereum.hex.HexData;
import io.emeraldpay.dshackle.upstream.ethereum.hex.HexEncoding;

import java.math.BigInteger;

/**
 * Utility class for Ethereum RPC JSON deserialization
 */
public abstract class EtherJsonDeserializer<T> extends JsonDeserializer<T> {

    protected String getHexString(JsonNode node) {
        if (node == null) {
            return null;
        }
        String value = node.textValue();
        if (value == null || value.length() == 0 || value.equals("0x")) {
            return null;
        }
        return value;
    }

    protected String getHexString(JsonNode node, String name) {
        return getHexString(node.get(name));
    }

    protected HexData getData(JsonNode node, String name) {
        String value = getHexString(node, name);
        if (value == null) return null;
        return HexData.from(value);
    }

    protected BigInteger getQuantity(JsonNode node, String name) {
        return getQuantity(node.get(name));
    }

    protected BigInteger getQuantity(JsonNode node) {
        if (node instanceof NumericNode) {
            return BigInteger.valueOf(node.longValue());
        }
        String value = getHexString(node);
        if (value == null) return null;
        if (!value.startsWith("0x")) {
            return new BigInteger(value, 10);
        }
        return HexEncoding.fromHex(value);
    }

    protected Integer getInt(JsonNode node, String name) {
        if (!node.has(name)) {
            return null;
        }
        return getInt(node.get(name));
    }

    protected Integer getInt(JsonNode node) {
        if (node instanceof NumericNode) {
            return node.intValue();
        }
        String hex = getHexString(node);
        if (hex == null || hex.length() <= 2) {
            return null;
        }
        return Integer.parseInt(hex.substring(2), 16);
    }

    protected Long getLong(JsonNode node, String name) {
        if (!node.has(name)) {
            return null;
        }
        return getLong(node.get(name));
    }

    protected Long getLong(JsonNode node) {
        if (node instanceof NumericNode) {
            return node.longValue();
        }
        String hex = getHexString(node);
        if (hex == null || hex.length() <= 2) {
            return null;
        }
        return Long.parseLong(hex.substring(2), 16);
    }

    protected Address getAddress(JsonNode node, String name) {
        String value = getHexString(node, name);
        if (value == null) return null;
        return Address.from(value);
    }

    protected TransactionId getTxHash(JsonNode node, String name) {
        String value = getHexString(node, name);
        if (value == null) return null;
        return TransactionId.from(value);
    }

    protected Wei getWei(JsonNode node, String name) {
        String value = getHexString(node, name);
        if (value == null) return null;
        return new Wei(HexEncoding.fromHex(value));
    }

    protected BlockHash getBlockHash(JsonNode node, String name) {
        String value = getHexString(node, name);
        if (value == null) return null;
        return BlockHash.from(value);
    }

    protected Boolean getBoolean(JsonNode node, String name) {
        if (!node.has(name)) {
            return null;
        }
        return node.get(name).asBoolean();
    }
}
