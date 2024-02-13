/*
 * Copyright (c) 2021 EmeraldPay Inc, All Rights Reserved.
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

import io.emeraldpay.etherjar.hex.Hex32;
import io.emeraldpay.etherjar.hex.HexData;
import org.bouncycastle.jcajce.provider.digest.Keccak;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

/**
 * A 32-byte long id representing a contract event, i.e. first item of the topics array in the log.
 * Calculated as keccac256("EventName(types...)")
 */
public class EventId extends Hex32 {

    public static final int SIZE_BYTES = Hex32.SIZE_BYTES;
    public static final int SIZE_HEX = Hex32.SIZE_HEX;

    private EventId(byte[] value) {
        super(value);
    }

    public static EventId from(byte[] value) {
        if (value == null)
            throw new IllegalArgumentException("Null Hash");

        if (value.length != SIZE_BYTES)
            throw new IllegalArgumentException("Invalid EventId length: " + value.length);

        return new EventId(value);
    }

    public static EventId from(String value) {
        if (value == null)
            throw new IllegalArgumentException("Null Hash");

        if (value.length() != SIZE_HEX)
            throw new IllegalArgumentException("Invalid EventId length: " + value.length());

        return new EventId(HexData.from(value).getBytes());
    }

    public static EventId empty() {
        return new EventId(new byte[SIZE_BYTES]);
    }

    public static EventId fromSignature(String name, String... types) {
        return fromSignature(name, Arrays.asList(types));
    }

    public static EventId fromSignature(String name, Collection<String> types) {
        String sign = Objects.requireNonNull(name) +
            '(' + String.join(",", Objects.requireNonNull(types)) + ')';
        Keccak.Digest256 digest256 = new Keccak.Digest256();
        digest256.update(sign.getBytes());
        return from(digest256.digest());
    }
}
