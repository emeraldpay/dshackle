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
import org.bouncycastle.jcajce.provider.digest.Keccak;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

/**
 * The first four bytes of the call data for a function call specifies the function to be called.
 *
 * <p>It is the first (left, high-order in big-endian) four bytes of the Keccak256 (SHA-3) hash
 * of the signature of the function.
 *
 * @see <a href="https://github.com/ethereum/wiki/wiki/Ethereum-Contract-ABI#function-selector">Function Selector</a>
 */
public class MethodId extends HexData {

    public static final int SIZE_BYTES = 4;
    public static final int SIZE_HEX = 2 + SIZE_BYTES * 2;

    public static MethodId fromSignature(String name, String... types) {
        return fromSignature(name, Arrays.asList(types));
    }

    public static MethodId fromSignature(String name, Collection<String> types) {
        String sign = Objects.requireNonNull(name) +
                '(' + String.join(",", Objects.requireNonNull(types)) + ')';

        byte[] head = new byte[SIZE_BYTES];
        Keccak.Digest256 digest256 = new Keccak.Digest256();

        digest256.update(sign.getBytes());
        System.arraycopy(digest256.digest(), 0, head, 0, SIZE_BYTES);

        return from(head);
    }

    public static MethodId from(byte[] value) {
        if (value == null)
            throw new IllegalArgumentException("Null Hash");

        if (value.length != SIZE_BYTES)
            throw new IllegalArgumentException("Invalid MethodId length: " + value.length);

        return new MethodId(value);
    }

    public static MethodId from(String value) {
        if (value == null)
            throw new IllegalArgumentException("Null Hash");

        if (value.length() != SIZE_HEX)
            throw new IllegalArgumentException("Invalid MethodId length: " + value.length());

        return new MethodId(HexData.from(value).getBytes());
    }

    public static MethodId empty() {
        return new MethodId(new byte[SIZE_BYTES]);
    }

    public static MethodId fromInput(HexData input) {
        if (input == null) {
            return null;
        }
        byte[] data = input.getBytes();
        if (data.length < SIZE_BYTES) {
            return null;
        }
        byte[] head = new byte[SIZE_BYTES];
        System.arraycopy(data, 0, head, 0, SIZE_BYTES);
        return new MethodId(head);
    }

    public MethodId(byte[] value) {
        super(value, SIZE_BYTES);
    }
}
