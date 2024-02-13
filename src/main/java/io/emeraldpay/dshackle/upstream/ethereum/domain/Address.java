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

import io.emeraldpay.etherjar.hex.Hex32;
import io.emeraldpay.etherjar.hex.HexData;
import org.bouncycastle.jcajce.provider.digest.Keccak;
import org.bouncycastle.util.encoders.Hex;

import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * Ethereum Wallet address
 */
public class Address extends HexData {

    private final static byte[] HEX_BYTES = "0123456789abcdef".getBytes();

    public static final int SIZE_BYTES = 20;
    public static final int SIZE_HEX = 2 + SIZE_BYTES * 2;

    private static final byte[] EMPTY_12BYTES = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

    /**
     * Use {@link Address#empty()}
     */
    @Deprecated
    public static final Address EMPTY = Address.from("0x0000000000000000000000000000000000000000");

    private static final Pattern CASE_INSENSITIVE_PATTERN = Pattern.compile("0x(?i:[0-9a-f]{40})");
    private static final Pattern CASE_SENSITIVE_PATTERN = Pattern.compile("0x(?:[0-9a-f]{40}|[0-9A-F]{40})");

    private Address(byte[] bytes) {
        super(bytes, SIZE_BYTES);
    }

    /**
     * Create address from the provided value
     *
     * @param value 20 byte data
     * @return address
     */
    public static Address from(HexData value) {
        if (value == null) {
            throw new IllegalArgumentException("Null input value");
        }
        if (value.getSize() != SIZE_BYTES) {
            throw new IllegalArgumentException("Invalid input length: " + value.getSize() + " != " + SIZE_BYTES);
        }
        return new Address(value.getBytes());
    }

    public static Address from(byte[] value) {
        if (value == null) {
            throw new IllegalArgumentException("Null input value");
        }
        if (value.length != SIZE_BYTES) {
            throw new IllegalArgumentException("Invalid input length: " + value.length + " != " + SIZE_BYTES);
        }
        return new Address(value);
    }

    public static Address from(String value) {
        if (value == null) {
            throw new IllegalArgumentException("Null input value");
        }
        if (value.length() != SIZE_HEX) {
            throw new IllegalArgumentException("Invalid input length: " + value.length() + " != " + SIZE_HEX);
        }
        return new Address(HexData.from(value).getBytes());
    }

    public static Address empty() {
        return new Address(new byte[SIZE_BYTES]);
    }

    /**
     * Extract address from Hex32 (i.e. from method call parameters, logs, etc). The extract method verifies
     * that the input in fact contains only address, i.e. input has zeroes for initial non-address bytes.
     *
     * @param value a Hex 32
     * @return address
     */
    public static Address extract(Hex32 value) {
        if (value == null) {
            throw new IllegalArgumentException("Null input value");
        }
        byte[] bytes = value.getBytes();
        byte[] empty = new byte[Hex32.SIZE_BYTES - Address.SIZE_BYTES];
        System.arraycopy(bytes, 0, empty, 0, empty.length);
        if (!Arrays.equals(empty, EMPTY_12BYTES)) {
            throw new IllegalArgumentException("Hex32 has non zero prefix for an Address");
        }
        byte[] address = new byte[Address.SIZE_BYTES];
        System.arraycopy(bytes, Hex32.SIZE_BYTES - Address.SIZE_BYTES, address,0, address.length);
        return new Address(address);
    }

    /**
     * Validate address according to EIP 55.
     *
     * @param address a wallet address ('0x...')
     * @return {@code true} if address correct or {@code false} otherwise
     * @see <a href="https://github.com/ethereum/EIPs/issues/55">EIP 55</a>
     */
    public static boolean isValidAddress(String address) {
        return CASE_INSENSITIVE_PATTERN.matcher(address).matches()
                && (CASE_SENSITIVE_PATTERN.matcher(address).matches() || isValidChecksum(address));
    }

    public String toChecksumString() {
        Keccak.Digest256 digest256 = new Keccak.Digest256();

        byte[] hex = new byte[value.length * 2];
        for(int i = 0, j = 0; i < value.length; i++){
            hex[j++] = HEX_BYTES[(0xF0 & value[i]) >>> 4];
            hex[j++] = HEX_BYTES[0x0F & value[i]];
        }
        digest256.update(hex);
        String hash = Hex.toHexString(digest256.digest());

        char[] plain = toHex().toCharArray();
        char[] str = new char[hex.length + 2];
        if (plain.length != str.length) {
            throw new IllegalStateException("Hex representation has invalid length: " + plain.length + " != " + str.length);
        }
        str[0] = '0';
        str[1] = 'x';
        for (int i = 0; i < 40; i++) {
            int dg = Character.digit(hash.charAt(i), 16);
            if (dg > 7) {
                str[i + 2] = Character.toUpperCase(plain[i + 2]);
            } else {
                str[i + 2] = plain[i + 2];
            }
        }
        return new String(str);
    }

    @Override
    public String toString() {
        return toChecksumString();
    }

    /**
     * Checks if the given string is an address with checksum (Keccak256).
     *
     * @param address a wallet address ('0x...')
     * @return {@code true} if address with checksum
     */
    static boolean isValidChecksum(String address) {
        Keccak.Digest256 digest256 = new Keccak.Digest256();

        digest256.update(
                address.substring(2).toLowerCase().getBytes());

        String hash = Hex.toHexString(digest256.digest());

        for (int i = 0; i < 40; i++) {
            char ch = address.charAt(i + 2);
            int dg = Character.digit(hash.charAt(i), 16);

            if ((dg > 7 && Character.toUpperCase(ch) != ch)
                    || (dg <= 7 && Character.toLowerCase(ch) != ch))
                return false;
        }

        return true;
    }
}
