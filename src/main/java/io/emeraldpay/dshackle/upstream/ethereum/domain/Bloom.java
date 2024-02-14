package io.emeraldpay.dshackle.upstream.ethereum.domain;

import io.emeraldpay.dshackle.upstream.ethereum.hex.HexData;
import org.bouncycastle.jcajce.provider.digest.Keccak;

import java.util.ArrayList;
import java.util.List;


public class Bloom extends HexData {

    public static final int SIZE_BYTES = 256;
    public static final int SIZE_HEX = 2 + SIZE_BYTES * 2;

    public Bloom(byte[] value) {
        super(value, SIZE_BYTES);
    }

    public static Bloom from(HexData value) {
        if (value == null) {
            throw new IllegalArgumentException("Null input value");
        }
        if (value.getSize() != SIZE_BYTES) {
            throw new IllegalArgumentException("Invalid input length: " + value.getSize() + " != " + SIZE_BYTES);
        }
        return new Bloom(value.getBytes());
    }

    public static Bloom from(String value) {
        if (value == null) {
            throw new IllegalArgumentException("Null input value");
        }
        if (value.length() != SIZE_HEX) {
            throw new IllegalArgumentException("Invalid input length: " + value.length() + " != " + SIZE_HEX);
        }
        return new Bloom(HexData.from(value).getBytes());
    }

    public static Bloom empty() {
        return new Bloom(new byte[SIZE_BYTES]);
    }

    public Bloom mergeWith(Bloom another) {
        byte[] result = new byte[SIZE_BYTES];
        for (int i = 0; i < SIZE_BYTES; i++) {
            result[i] = (byte)(this.value[i] | another.value[i]);
        }
        return new Bloom(result);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static int bytePosition(int bitpos) {
        return bitpos >> 3;
    }

    public static int byteMask(int bytepos, int bitpos) {
        int inbytepos = bitpos - (bytepos << 3);
        return 1 << (inbytepos);
    }

    public static class Builder {
        private final byte[] current = new byte[SIZE_BYTES];
        private final Keccak.Digest256 keccak = new Keccak.Digest256();

        public Builder add(HexData value) {
            keccak.update(value.getBytes());
            byte[] hash = keccak.digest();

            for (int i = 0; i < 6; i+= 2) {
                int high = hash[i] & 0x7;
                int low  = hash[i + 1] & 0xff;
                int bitpos = (high << 8) + low;
                int bytepos = bytePosition(bitpos);
                int bytemask = byteMask(bytepos, bitpos);
                current[SIZE_BYTES - bytepos - 1] |= bytemask & 0xff;
            }
            return this;
        }

        public Bloom build() {
            return new Bloom(current);
        }

        public Filter buildFilter() {
            List<Filter.Check> checks = new ArrayList<>();
            for (int i = 0; i < SIZE_BYTES; i++) {
                if (current[i] != 0) {
                    checks.add(new Filter.Check(i, current[i]));
                }
            }
            return new Filter(checks.toArray(new Filter.Check[0]));
        }
    }

    public static class Filter {
        private final Check[] checks;

        public Filter(Check[] checks) {
            this.checks = checks;
        }

        public boolean isSet(Bloom bloom) {
            for (Check check: checks) {
                int actual = bloom.value[check.pos] & check.mask;
                if (actual == 0) {
                    return false;
                }
            }
            return true;
        }

        public static class Check {
            private final int pos;
            private final byte mask;

            public Check(int pos, byte mask) {
                if (pos >= SIZE_BYTES) {
                    throw new IllegalArgumentException("Out of filter boundaries position: " + pos);
                }
                if (mask == 0) {
                    throw new IllegalArgumentException("Mask cannot be empty");
                }
                this.pos = pos;
                this.mask = mask;
            }
        }
    }
}
