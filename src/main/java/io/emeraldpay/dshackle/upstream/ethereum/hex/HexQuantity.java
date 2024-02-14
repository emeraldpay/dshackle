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
package io.emeraldpay.dshackle.upstream.ethereum.hex;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Objects;

public class HexQuantity implements Serializable {

    private final BigInteger value;

    public HexQuantity(BigInteger value) {
        if (value == null) {
            throw new IllegalArgumentException("Can't create instance of null quantity");
        }
        this.value = value;
    }

    public static HexQuantity from(Long value) {
        if (value == null) {
            return null;
        }
        return new HexQuantity(BigInteger.valueOf(value));
    }

    public static HexQuantity from(BigInteger value) {
        if (value == null) {
            return null;
        }
        return new HexQuantity(value);
    }

    public static HexQuantity from(String value) {
        if (value == null) {
            return null;
        }
        int signum = 1;
        if (value.startsWith("-")) {
            signum = -1;
            value = value.substring(1);
        }
        if (!value.startsWith("0x")) {
            throw new IllegalArgumentException("Input must be formatted as a hex value");
        }
        value = value.substring(2);
        if (value.length() == 0) {
            return null;
        }
        try {
            BigInteger num = new BigInteger(value, 16);
            if (signum == -1) {
                num = num.negate();
            }
            return new HexQuantity(num);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid quantity value: " + value);
        }
    }

    public String toHex() {
        return (value.signum() == -1 ? "-" : "") + "0x" + value.abs().toString(16);
    }

    public String toString() {
        return toHex();
    }

    public BigInteger getValue() {
        return value;
    }

    public HexData asData() {
        return new HexData(value.toByteArray());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HexQuantity that = (HexQuantity) o;
        return value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
}
