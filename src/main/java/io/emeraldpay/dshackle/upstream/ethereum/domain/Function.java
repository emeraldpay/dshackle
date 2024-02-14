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
 * An address, followed by a function selector.
 */
public class Function extends HexData {

    public static final int SIZE_BYTES = 24;
    public static final int SIZE_HEX = 2 + SIZE_BYTES * 2;

    private Function(byte[] bytes) {
        super(bytes, SIZE_BYTES);
    }

    public static Function from(byte[] value) {
        if (value == null) {
            throw new IllegalArgumentException("Null Function");
        }
        if (value.length != SIZE_BYTES) {
            throw new IllegalArgumentException("Invalid Function length: " + value.length);
        }
        return new Function(value);
    }

    public static Function from(String value) {
        if (value == null) {
            throw new IllegalArgumentException("Null Function");
        }
        if (value.length() != SIZE_HEX) {
            throw new IllegalArgumentException("Invalid Function length: " + value.length());
        }
        return new Function(HexData.from(value).getBytes());
    }
}
