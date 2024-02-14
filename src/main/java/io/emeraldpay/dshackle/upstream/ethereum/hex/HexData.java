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

package io.emeraldpay.dshackle.upstream.ethereum.hex;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Fixed size value, such as Wallet Address, represented in Hex.
 */
public class HexData implements Serializable {

    /**
     * Use {@link HexData#empty()}
     */
    @Deprecated
    public final static HexData EMPTY = new HexData(new byte[0]);

    private final static char[] HEX_DIGITS = "0123456789abcdef".toCharArray();

    /**
     * Combine an array of hex data into single instance.
     *
     * @param data an array of hex data
     * @return a hex data
     * @see #combine(Collection)
     */
    public static HexData combine(HexData... data) {
        return combine(Arrays.asList(data));
    }

    /**
     * Combine a collection of hex data into single instance.
     *
     * @param data a collection of hex data
     * @return a hex data
     * @see #combine(HexData...)
     */
    public static HexData combine(Collection<? extends HexData> data) {
        return empty().concat(data);
    }

    public static HexData from(long value) {
        return new HexData(BigInteger.valueOf(value).toByteArray());
    }

    /**
     * Parse hex representation for a number, should start with {@code 0x}.
     *
     * @param value hex value
     * @return parsed value
     */
    public static HexData from(String value) {
        if (value.isEmpty())
            throw new IllegalArgumentException("Empty value");

        if (!value.startsWith("0x"))
            throw new IllegalArgumentException("Invalid hex format: " + value);

        value = value.substring(2);

        if (value.length() <= 0) return empty();

        byte[] bytes = new BigInteger(value, 16).toByteArray();

        int len = (value.length() / 2) + (value.length() % 2);

        if (bytes.length == len)
            return new HexData(bytes);

        byte[] buf = new byte[len];

        // for values like 0xffffff it produces extra 0 byte in the beginning, we need to skip it
        int pos = bytes.length > buf.length ? bytes.length - buf.length : 0;
        System.arraycopy(bytes, pos, buf, buf.length - bytes.length + pos, bytes.length - pos);

        return new HexData(buf);
    }

    public static HexData empty(int size) {
        if (size < 0) {
            throw new IllegalArgumentException("Size cannot be less than zero");
        }
        return new HexData(new byte[size]);
    }

    public static HexData empty() {
        return empty(0);
    }

    protected final byte[] value;

    public HexData(byte[] value) {
        this(value, value.length);
    }

    public HexData(byte[] value, int size) {
        if (value.length != size)
            throw new IllegalArgumentException("Invalid data size: " + value.length);

        this.value = value;
    }

    /**
     * Concat with an array of {@link HexData}.
     *
     * @param data an array of {@link HexData}
     * @return hex value
     * @see #concat(Collection)
     */
    public HexData concat(HexData... data) {
        return concat(Arrays.asList(data));
    }

    /**
     * Concat with a collection of {@link HexData}.
     *
     * @param data a collection of {@link HexData}
     * @return hex value
     * @see #concat(HexData...)
     */
    public HexData concat(Collection<? extends HexData> data) {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();

        try {
            buf.write(getBytes());

            for (HexData param: data) {
                buf.write(param.getBytes());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return new HexData(buf.toByteArray());
    }

    /**
     * Extract a {@link HexData} instance by required {@code size} bytes from start.
     *
     * @param size a size in bytes
     * @return an extracted {@link HexData} instance
     * @see #extract(int, int)
     */
    public HexData extract(int size) {
        return extract(size, 0, Function.identity());
    }

    /**
     * Extract a new object by required {@code size} bytes from start.
     *
     * @param size a size in bytes
     * @param conv a converter from hex data into required object type
     * @param <T> a java object type is needed to return
     * @return an extracted object
     *
     * @see #extract(int, int, Function)
     */
    public <T> T extract(int size, Function<? super HexData, T> conv) {
        return extract(size, 0, conv);
    }

    /**
     * Extract a {@link HexData} instance by required {@code size} bytes
     * and with {@code offset}.
     *
     * @param size a size in bytes
     * @param offset an offset in bytes
     * @return an extracted {@link HexData} instance
     * @see #extract(int, int, Function)
     */
    public HexData extract(int size, int offset) {
        return extract(size, offset, Function.identity());
    }

    /**
     * Extract tail of the underlying data, skipping {@code offset} bytes.
     *
     * @param offset size in bytes to skip
     * @return extracted data
     */
    public HexData skip(int offset) {
        if (offset == 0) {
            return this;
        }
        if (offset > this.value.length) {
            throw new IndexOutOfBoundsException("Cannot skip " + offset + " of " + this.value.length);
        }
        if (offset == this.value.length) {
            return HexData.empty();
        }
        return extract(this.value.length - offset, offset);
    }

    /**
     * Extract a new object by required {@code size} bytes
     * and from {@code offset}.
     *
     * @param size a size in bytes
     * @param offset an offset in bytes
     * @param conv a converter from hex data into required object type
     * @param <T> a java object type is needed to return
     * @return an extracted object
     * @see #extract(int, Function)
     */
    public <T> T extract(int size, int offset, Function<? super HexData, T> conv) {
        Objects.requireNonNull(conv);

        if (size < 0 || offset < 0)
            throw new IllegalArgumentException("Negative extract arguments");

        if (getSize() < size + offset)
            throw new IllegalArgumentException("Insufficient size to extract");

        if (size == 0)
            return conv.apply(empty());

        return conv.apply(
                new HexData(Arrays.copyOfRange(value, offset, size + offset)));
    }

    /**
     * Returns an array of {@link HexData} were split by required {@code size}
     * bytes from start.
     *
     * @param size a size in bytes to split by
     * @return an array of split hex data
     * @throws IllegalArgumentException if the hex data length
     * is not a multiple of given {@code size}
     * @see #split(int, int)
     */
    public HexData[] split(int size) {
        return split(size, 0, HexData[]::new, Function.identity());
    }

    /**
     * Returns an array of {@link Hex32} by splitting the value bytes
     *
     * @return an array of split 32-bytes data
     * @throws IllegalArgumentException if the summary length to split
     * is not a multiple of given {@code size}
     * @see #split32(int)
     */
    public Hex32[] split32() {
        return split32(0);
    }

    /**
     * Returns an array of the elements were split by required {@code size}
     * bytes from start.
     *
     * @param size a size in bytes to split by
     * @param gen a function which produces a new array of the desired
     *            type and the provided length
     * @param conv a converter from hex data into required object type
     * @param <T> the element type of the resulting array
     * @return an array of split type instances
     * @throws IllegalArgumentException if the hex data length
     * is not a multiple of given {@code size}
     * @see #split(int, int, IntFunction, Function)
     */
    public <T> T[] split(int size, IntFunction<T[]> gen, Function<? super HexData, T> conv) {
        return split(size, 0, gen, conv);
    }

    /**
     * Returns an array of {@link HexData} were split by required {@code size}
     * bytes and from {@code offset}.
     *
     * @param size a size in bytes to split by
     * @param offset an offset in bytes to split from
     * @return an array of split hex data
     * @throws IllegalArgumentException if the summary length to split
     * is not a multiple of given {@code size}
     * @see #split(int)
     */
    public HexData[] split(int size, int offset) {
        return split(size, offset, HexData[]::new, Function.identity());
    }

    /**
     * Returns an array of {@link Hex32} by splitting the value bytes starting from {@code offset}.
     *
     * @param offset an offset in bytes to split from
     * @return an array of split 32-bytes data
     * @throws IllegalArgumentException if the summary length to split
     * is not a multiple of given {@code size}
     * @see #split32()
     */
    public Hex32[] split32(int offset) {
        return split(Hex32.SIZE_BYTES, offset, Hex32[]::new, v -> Hex32.from(v.value));
    }

    /**
     * Returns an array of the elements were split by required {@code size}
     * bytes and from {@code offset}, using the provided {@code conv}
     * function to convert {@link HexData} into required object type.
     *
     * <pre>{@code
     *     List<String> coll = data.split(32, 0, String[]::new, data::toHex);
     * }</pre>
     *
     * @param size a size in bytes to split by
     * @param offset an offset in bytes to split from
     * @param gen a function which produces a new array of the desired
     *            type and the provided length
     * @param conv a converter from hex data into required object type
     * @param <T> the element type of the resulting array
     * @return an array of split type instances
     * @throws IllegalArgumentException if the summary length to split
     * is not a multiple of given {@code size}
     * @see #split(int, IntFunction, Function)
     */
    public <T> T[] split(int size, int offset, IntFunction<T[]> gen, Function<? super HexData, T> conv) {
        Objects.requireNonNull(conv);

        if (size < 0 || offset < 0)
            throw new IllegalArgumentException("Negative extract arguments");

        if (getSize() < offset)
            throw new IllegalArgumentException("Insufficient size to extract");

        if (size != 0 && (getSize() - offset) % size != 0)
            throw new IllegalArgumentException("Length to split is not a multiple of " + size);

        if (size == 0)
            return gen.apply(0);

        Stream<byte[]> stream = IntStream.range(0, (getSize() - offset) / size)
                .map(i -> i * size + offset).mapToObj(j -> Arrays.copyOfRange(value, j, j + size));

        return stream.map(HexData::new).map(conv::apply).toArray(gen);
    }

    public String toHex() {
        char[] hex = new char[value.length * 2 + 2];

        hex[0] = '0';
        hex[1] = 'x';

        for(int i = 0, j = 2; i < value.length; i++){
            hex[j++] = HEX_DIGITS[(0xF0 & value[i]) >>> 4];
            hex[j++] = HEX_DIGITS[0x0F & value[i]];
        }

        return new String(hex);
    }

    public HexQuantity asQuantity() {
        return new HexQuantity(new BigInteger(1, value));
    }

    /**
     * Try to extract an array of Hex32 packed into the value. The array is encoded with 32 bytes of offset
     * value, 32 bytes of length and following items.
     *
     * @return array of values
     * @throws IllegalArgumentException if invalid structure or length
     */
    public Hex32[] asEncodedArray() {
        HexData[] parts = split(Hex32.SIZE_BYTES);
        if (parts.length < 2) {
            throw new IllegalArgumentException("Not an encoded array");
        }
        Hex32 _offset = Hex32.from(parts[0]);
        int len = parts[1].asQuantity().getValue().intValue();
        if (parts.length != 2 + len) {
            throw new IllegalArgumentException("Invalid data length. " + parts.length + " != " + (2 + len));
        }
        Hex32[] result = new Hex32[len];
        for (int i = 0; i < len; i++) {
            result[i] = Hex32.from(parts[2 + i]);
        }
        return result;
    }

    /**
     * Try to extract an array of &lt;T&gt; packed into the value. The array is encoded with 32 bytes of offset
     * value, 32 bytes of length and following items.
     *
     * The conversion function must extract actual value (ex. an Address, or Number) from the Hex32 representation.
     *
     * @param converter conversion function to extract actual value
     * @param <T> target type
     * @return array of values
     * @throws IllegalArgumentException if invalid structure or length
     */
    @SuppressWarnings("unchecked")
    public <T> T[] asEncodedArray(Function<Hex32, T> converter) {
        Hex32[] values = asEncodedArray();
        return (T[]) Arrays.stream(values).map(converter).toArray();
    }

    public String toString() {
        return toHex();
    }

    public byte[] getBytes() {
        return value.clone();
    }

    public int getSize() {
        return value.length;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null) return false;
        if (!(o instanceof HexData)) return false;

        HexData hexData = (HexData) o;

        return Arrays.equals(value, hexData.value);
    }
}
