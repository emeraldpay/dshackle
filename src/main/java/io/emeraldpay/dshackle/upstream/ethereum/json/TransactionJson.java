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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.emeraldpay.dshackle.upstream.ethereum.domain.Address;
import io.emeraldpay.dshackle.upstream.ethereum.domain.BlockHash;
import io.emeraldpay.dshackle.upstream.ethereum.domain.TransactionRef;
import io.emeraldpay.dshackle.upstream.ethereum.domain.TransactionSignature;
import io.emeraldpay.dshackle.upstream.ethereum.domain.Wei;
import io.emeraldpay.dshackle.upstream.ethereum.hex.Hex32;
import io.emeraldpay.dshackle.upstream.ethereum.hex.HexData;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@JsonDeserialize(using = TransactionJsonDeserializer.class)
@JsonSerialize(using = TransactionJsonSerializer.class)
public class TransactionJson extends TransactionRefJson implements TransactionRef, Serializable {

    /**
     * the number of transactions made by the sender prior to this one.
     */
    private Long nonce;

    /**
     * hash of the block where this transaction was in. null when its pending.
     */
    private BlockHash blockHash;

    /**
     * block number where this transaction was in. null when its pending.
     */
    private Long blockNumber;

    /**
     * position in the block. null when its pending.
     */
    private Long transactionIndex;

    /**
     * address of the sender.
     */
    private Address from;

    /**
     * address of the receiver. null when its a contract creation transaction.
     */
    private Address to;

    /**
     * Address of a contract created from that transaction
     */
    private Address creates;

    /**
     * value transferred in Wei.
     */
    private Wei value;

    /**
     * gas price provided by the sender in Wei.
     */
    private Wei gasPrice;
    private Wei maxFeePerGas;
    private Wei maxPriorityFeePerGas;

    /**
     * gas provided by the sender.
     */
    private Long gas;

    /**
     * the data send along with the transaction.
     */
    private HexData input;

    private TransactionSignature signature;

    /**
     * Transaction type
     * @see <a href="https://eips.ethereum.org/EIPS/eip-2718">EIP-2718: Typed Transaction Envelope</a>
     */
    private int type = 0;

    private Integer chainId;

    private List<Access> accessList;

    public Long getNonce() {
        return nonce;
    }

    public void setNonce(Long nonce) {
        this.nonce = nonce;
    }

    public BlockHash getBlockHash() {
        return blockHash;
    }

    public void setBlockHash(BlockHash blockHash) {
        this.blockHash = blockHash;
    }

    public Long getBlockNumber() {
        return blockNumber;
    }

    public void setBlockNumber(Long blockNumber) {
        this.blockNumber = blockNumber;
    }

    public Long getTransactionIndex() {
        return transactionIndex;
    }

    public void setTransactionIndex(Long transactionIndex) {
        this.transactionIndex = transactionIndex;
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

    public Wei getValue() {
        return value;
    }

    public void setValue(Wei value) {
        this.value = value;
    }

    public Wei getGasPrice() {
        return gasPrice;
    }

    public void setGasPrice(Wei gasPrice) {
        this.gasPrice = gasPrice;
    }

    public Wei getMaxFeePerGas() {
        return maxFeePerGas;
    }

    public void setMaxFeePerGas(Wei maxFeePerGas) {
        this.maxFeePerGas = maxFeePerGas;
    }

    public Wei getMaxPriorityFeePerGas() {
        return maxPriorityFeePerGas;
    }

    public void setMaxPriorityFeePerGas(Wei maxPriorityFeePerGas) {
        this.maxPriorityFeePerGas = maxPriorityFeePerGas;
    }

    public Integer getChainId() {
        return chainId;
    }

    public void setChainId(Integer chainId) {
        this.chainId = chainId;
    }

    public Long getGas() {
        return gas;
    }

    public void setGas(Long gas) {
        this.gas = gas;
    }

    public HexData getInput() {
        return input;
    }

    public void setInput(HexData input) {
        this.input = input;
    }

    public TransactionSignature getSignature() {
        return signature;
    }

    public void setSignature(TransactionSignature signature) {
        this.signature = signature;
    }

    public Address getCreates() {
        return creates;
    }

    public void setCreates(Address creates) {
        this.creates = creates;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public List<Access> getAccessList() {
        return accessList;
    }

    public void setAccessList(List<Access> accessList) {
        this.accessList = accessList;
    }

    public void addAccess(Access access) {
        if (this.accessList == null) {
            this.accessList = new ArrayList<>();
        }
        accessList.add(access);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TransactionJson)) return false;

        TransactionJson that = (TransactionJson) o;

        if (!Objects.equals(getHash(), that.getHash())) return false;
        if (!Objects.equals(nonce, that.nonce)) return false;
        if (!Objects.equals(blockHash, that.blockHash)) return false;
        if (!Objects.equals(blockNumber, that.blockNumber)) return false;
        if (!Objects.equals(transactionIndex, that.transactionIndex)) return false;
        if (!Objects.equals(from, that.from)) return false;
        if (!Objects.equals(to, that.to)) return false;
        if (!Objects.equals(value, that.value)) return false;
        if (!Objects.equals(gasPrice, that.gasPrice)) return false;
        if (!Objects.equals(gas, that.gas)) return false;
        if (!Objects.equals(input, that.input)) return false;
        if (!Objects.equals(creates, that.creates)) return false;
        if (type != that.type) return false;
        if (!Objects.equals(maxFeePerGas, that.maxFeePerGas)) return false;
        if (!Objects.equals(maxPriorityFeePerGas, that.maxPriorityFeePerGas)) return false;
        if (!Objects.equals(accessList, that.accessList)) return false;
        return Objects.equals(signature, that.signature);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + type;
        result = 31 * result + (from != null ? from.hashCode() : 0);
        result = 31 * result + (blockHash != null ? blockHash.hashCode() : 0);
        return result;
    }

    public static class Access {
        private Address address;
        private List<Hex32> storageKeys;

        public Access() {
            storageKeys = Collections.emptyList();
        }

        public Access(Address address) {
            this();
            this.address = address;
        }

        public Access(Address address, List<Hex32> storageKeys) {
            this.address = address;
            this.storageKeys = storageKeys;
        }

        public Address getAddress() {
            return address;
        }

        public void setAddress(Address address) {
            this.address = address;
        }

        public List<Hex32> getStorageKeys() {
            return storageKeys;
        }

        public void setStorageKeys(List<Hex32> storageKeys) {
            this.storageKeys = storageKeys;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Access)) return false;
            Access that = (Access) o;
            return Objects.equals(address, that.address) && Objects.equals(storageKeys, that.storageKeys);
        }

        @Override
        public int hashCode() {
            return Objects.hash(address);
        }
    }
}
