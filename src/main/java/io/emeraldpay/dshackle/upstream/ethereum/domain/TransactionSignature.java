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

import io.emeraldpay.etherjar.hex.HexData;

import java.util.Objects;

/**
 * Transaction signature with support of Replay Protection (EIP-155)
 */
public class TransactionSignature {

    private ChainId chainId;

    private HexData publicKey;
    private HexData r;
    private HexData s;
    private Long v;

    public TransactionSignature() {
    }

    public ChainId getChainId() {
        return chainId;
    }

    public void setChainId(ChainId chainId) {
        this.chainId = chainId;
    }

    public HexData getPublicKey() {
        return publicKey;
    }

    public void setPublicKey(HexData publicKey) {
        this.publicKey = publicKey;
    }

    public HexData getR() {
        return r;
    }

    public void setR(HexData r) {
        this.r = r;
    }

    public HexData getS() {
        return s;
    }

    public void setS(HexData s) {
        this.s = s;
    }

    public Long getV() {
        return v;
    }

    public void setV(Long v) {
        if (v == null || v < 0) {
            throw new IllegalArgumentException("Invalid V: " + v);
        }
        this.v = v;
    }

    public ChainId getExtractedChainId() {
        if (!isProtected()) {
            return null;
        }
        return new ChainId((v - 35) / 2);
    }

    public Long getNormalizedV() {
        if (chainId == null) {
            return v;
        }
        return v - chainId.getValue() * 2 - 35 + 27;
    }

    public boolean isProtected() {
        if (v == 27 || v == 28) {
            return false;
        }
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TransactionSignature)) return false;
        TransactionSignature that = (TransactionSignature) o;
        return Objects.equals(chainId, that.chainId) && Objects.equals(publicKey, that.publicKey) && Objects.equals(r, that.r) && Objects.equals(s, that.s) && Objects.equals(v, that.v);
    }

    @Override
    public int hashCode() {
        return Objects.hash(chainId, s, v);
    }
}
