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
import io.emeraldpay.dshackle.upstream.ethereum.domain.TransactionId;
import io.emeraldpay.dshackle.upstream.ethereum.domain.TransactionRef;
import io.emeraldpay.dshackle.upstream.ethereum.hex.Hex32;
import io.emeraldpay.dshackle.upstream.ethereum.hex.HexData;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

@JsonDeserialize(using = TransactionLogJsonDeserializer.class)
@JsonSerialize(using = TransactionLogJsonSerializer.class)
public class TransactionLogJson implements TransactionRef, Serializable {

    /**
     * true when the log was removed, due to a chain reorganization. false if its a valid log.
     */
    private Boolean removed;

    /**
     * log index position in the block. null when its pending log.
     */
    private Long logIndex;

    /**
     * transactions index position log was created from. null when its pending log.
     */
    private Long transactionIndex;

    /**
     * hash of the transactions this log was created from. null when its pending log.
     */
    private TransactionId transactionHash;

    /**
     * hash of the block where this log was in. null when its pending. null when its pending log.
     */
    private BlockHash blockHash;

    /**
     * the block number where this log was in. null when its pending. null when its pending log.
     */
    private Long blockNumber;

    /**
     * address from which this log originated.
     */
    private Address address;

    /**
     * contains one or more 32 Bytes non-indexed arguments of the log.
     */
    private HexData data;

    /**
     * Array of 0 to 4 32 Bytes DATA of indexed log arguments.
     *
     * In solidity: The first topic is the hash of the signature of the event (e.g. Deposit(address,bytes32,uint256)),
     * except you declared the event with the anonymous specifier.
     */
    private List<Hex32> topics;

    public Boolean getRemoved() {
        return removed;
    }

    public void setRemoved(Boolean removed) {
        this.removed = removed;
    }

    public Long getLogIndex() {
        return logIndex;
    }

    public void setLogIndex(Long logIndex) {
        this.logIndex = logIndex;
    }

    public Long getTransactionIndex() {
        return transactionIndex;
    }

    public void setTransactionIndex(Long transactionIndex) {
        this.transactionIndex = transactionIndex;
    }

    public TransactionId getTransactionHash() {
        return transactionHash;
    }

    public void setTransactionHash(TransactionId transactionHash) {
        this.transactionHash = transactionHash;
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

    public Address getAddress() {
        return address;
    }

    public void setAddress(Address address) {
        this.address = address;
    }

    public HexData getData() {
        return data;
    }

    public void setData(HexData data) {
        this.data = data;
    }

    public List<Hex32> getTopics() {
        return topics;
    }

    public void setTopics(List<Hex32> topics) {
        this.topics = topics;
    }

    @Override
    public TransactionId getHash() {
        return transactionHash;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TransactionLogJson)) return false;

        TransactionLogJson that = (TransactionLogJson) o;

        if (!Objects.equals(removed, that.removed)) return false;
        if (!Objects.equals(logIndex, that.logIndex)) return false;
        if (!Objects.equals(transactionIndex, that.transactionIndex))
            return false;
        if (!Objects.equals(transactionHash, that.transactionHash))
            return false;
        if (!Objects.equals(blockHash, that.blockHash)) return false;
        if (!Objects.equals(blockNumber, that.blockNumber)) return false;
        if (!Objects.equals(address, that.address)) return false;
        if (!Objects.equals(data, that.data)) return false;
        return Objects.equals(topics, that.topics);
    }

    @Override
    public int hashCode() {
        int result = logIndex != null ? logIndex.hashCode() : 0;
        result = 31 * result + (transactionHash != null ? transactionHash.hashCode() : 0);
        result = 31 * result + (blockHash != null ? blockHash.hashCode() : 0);
        return result;
    }
}
