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

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.emeraldpay.dshackle.upstream.ethereum.domain.Address;
import io.emeraldpay.dshackle.upstream.ethereum.domain.Wei;
import io.emeraldpay.dshackle.upstream.ethereum.hex.HexData;

import java.io.Serializable;

@JsonSerialize(using = TransactionCallJsonSerializer.class)
public class TransactionCallJson implements Serializable {

    private Address from;
    private Address to;
    private Long gas;
    private Wei gasPrice;
    private Wei value;
    private HexData data;
    private Long nonce;

    public TransactionCallJson() {
    }

    public TransactionCallJson(Address to, HexData data) {
        this.to = to;
        this.data = data;
    }

    public TransactionCallJson(Address from, Address to, Wei value) {
        this.from = from;
        this.to = to;
        this.value = value;
    }

    public TransactionCallJson(Address from, Address to, Long gas, Wei value, HexData data) {
        this.from = from;
        this.to = to;
        this.gas = gas;
        this.value = value;
        this.data = data;
    }

    public Address getFrom() {
        return from;
    }

    public void setFrom(Address from) {
        this.from = from;
    }

    public Address getTo() {
        return to;
    }

    public void setTo(Address to) {
        this.to = to;
    }

    public Long getGas() {
        return gas;
    }

    public void setGas(Long gas) {
        this.gas = gas;
    }

    public Wei getGasPrice() {
        return gasPrice;
    }

    public void setGasPrice(Wei gasPrice) {
        this.gasPrice = gasPrice;
    }

    public Wei getValue() {
        return value;
    }

    public void setValue(Wei value) {
        this.value = value;
    }

    public HexData getData() {
        return data;
    }

    public void setData(HexData data) {
        this.data = data;
    }

    public Long getNonce() {
        return nonce;
    }

    public void setNonce(Long nonce) {
        this.nonce = nonce;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TransactionCallJson)) return false;

        TransactionCallJson that = (TransactionCallJson) o;

        if (from != null ? !from.equals(that.from) : that.from != null) return false;
        if (!to.equals(that.to)) return false;
        if (gas != null ? !gas.equals(that.gas) : that.gas != null) return false;
        if (gasPrice != null ? !gasPrice.equals(that.gasPrice) : that.gasPrice != null) return false;
        if (value != null ? !value.equals(that.value) : that.value != null) return false;
        if (nonce != null ? !nonce.equals(that.nonce) : that.nonce != null) return false;
        return data != null ? data.equals(that.data) : that.data == null;
    }

    @Override
    public int hashCode() {
        int result = from != null ? from.hashCode() : 0;
        result = 31 * result + to.hashCode();
        result = 31 * result + (data != null ? data.hashCode() : 0);
        return result;
    }
}
