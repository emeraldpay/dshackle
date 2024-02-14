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

package io.emeraldpay.dshackle.upstream.ethereum.domain;

import io.emeraldpay.dshackle.upstream.ethereum.hex.HexData;

/**
 * Transaction Hash value
 */
public class TransactionId extends HexData {

    public static final int SIZE_BYTES = 32;
    public static final int SIZE_HEX = 2 + SIZE_BYTES * 2;

    protected TransactionId(byte[] value) {
        super(value, SIZE_BYTES);
    }

    /**
     * Parse value from bytes representation. Value must be 32 bytes long.
     *
     * @param value bytes representation
     * @return TransactionId
     */
    public static TransactionId from(byte[] value) {
        if (value.length != SIZE_BYTES) {
            throw new IllegalArgumentException("Invalid Tx length: " + value.length);
        }
        return new TransactionId(value);
    }

    /**
     * Parse value from hex representation. Value must be 64 characters long.
     *
     * @param value bytes representation
     * @return TransactionId
     */
    public static TransactionId from(String value) {
        if (value == null) {
            return null;
        }
        if (value.length() != SIZE_HEX) {
            throw new IllegalArgumentException("Invalid Tx length: " + value.length());
        }
        return new TransactionId(HexData.from(value).getBytes());
    }

    public static TransactionId empty() {
        return new TransactionId(new byte[SIZE_BYTES]);
    }

}
