package io.emeraldpay.dshackle.upstream.ethereum.json;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.emeraldpay.dshackle.upstream.ethereum.domain.Address;
import io.emeraldpay.dshackle.upstream.ethereum.domain.BlockHash;
import io.emeraldpay.dshackle.upstream.ethereum.domain.Bloom;
import io.emeraldpay.dshackle.upstream.ethereum.domain.Wei;
import io.emeraldpay.dshackle.upstream.ethereum.hex.HexData;

import java.io.Serializable;
import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@JsonDeserialize(using = BlockJsonDeserializer.class)
@JsonSerialize(using = BlockJsonSerializer.class)
public class BlockJson<T extends TransactionRefJson> implements Serializable {

    /**
     * the block number. null when its pending block.
     */
    private Long number;

    /**
     * hash of the block. null when its pending block.
     */
    private BlockHash hash;

    /**
     * hash of the parent block.
     */
    private BlockHash parentHash;

    /**
     * SHA3 of the uncles data in the block.
     */
    private HexData sha3Uncles;

    /**
     * the bloom filter for the logs of the block. null when its pending block.
     */
    private Bloom logsBloom;

    /**
     * the root of the transaction trie of the block.
     */
    private HexData transactionsRoot;

    /**
     * the root of the final state trie of the block.
     */
    private HexData stateRoot;

    /**
     * the root of the receipts trie of the block.
     */
    private HexData receiptsRoot;

    /**
     * the address of the beneficiary to whom the mining rewards were given.
     */
    private Address miner;

    /**
     * the difficulty for this block.
     */
    private BigInteger difficulty;

    /**
     * total difficulty of the chain until this block.
     */
    private BigInteger totalDifficulty;

    /**
     * the "extra data" field of this block.
     */
    private HexData extraData;

    /**
     * the size of this block in bytes.
     */
    private Long size;

    /**
     * the maximum gas allowed in this block.
     */
    private Long gasLimit;

    /**
     * the total used gas by all transactions in this block.
     */
    private Long gasUsed;

    /**
     * when the block was collated
     */
    private Instant timestamp;

    /**
     * List of transaction objects, or 32 Bytes transaction hashes depending on the last given parameter.
     *
     * HexData or TransactionJson
     */
    private List<T> transactions;

    /**
     * list of uncle hashes.
     */
    private List<BlockHash> uncles;

    /**
     * basefee value for that block, as per EIP-1559
     */
    private Wei baseFeePerGas;


    private HexData mixHash;

    private HexData nonce;

    private HexData withdrawalsRoot;

    public HexData getMixHash() {
        return mixHash;
    }

    public void setMixHash(HexData mixHash) {
        this.mixHash = mixHash;
    }

    public HexData getNonce() {
        return nonce;
    }

    public void setNonce(HexData nonce) {
        this.nonce = nonce;
    }

    public HexData getWithdrawalsRoot() {
        return withdrawalsRoot;
    }

    public void setWithdrawalsRoot(HexData withdrawalsRoot) {
        this.withdrawalsRoot = withdrawalsRoot;
    }

    public Long getNumber() {
        return number;
    }

    public void setNumber(Long number) {
        this.number = number;
    }

    public BlockHash getHash() {
        return hash;
    }

    public void setHash(BlockHash hash) {
        this.hash = hash;
    }

    public BlockHash getParentHash() {
        return parentHash;
    }

    public void setParentHash(BlockHash parentHash) {
        this.parentHash = parentHash;
    }

    public HexData getSha3Uncles() {
        return sha3Uncles;
    }

    public void setSha3Uncles(HexData sha3Uncles) {
        this.sha3Uncles = sha3Uncles;
    }

    public Bloom getLogsBloom() {
        return logsBloom;
    }

    public void setLogsBloom(Bloom logsBloom) {
        this.logsBloom = logsBloom;
    }

    public HexData getTransactionsRoot() {
        return transactionsRoot;
    }

    public void setTransactionsRoot(HexData transactionsRoot) {
        this.transactionsRoot = transactionsRoot;
    }

    public HexData getStateRoot() {
        return stateRoot;
    }

    public void setStateRoot(HexData stateRoot) {
        this.stateRoot = stateRoot;
    }

    public HexData getReceiptsRoot() {
        return receiptsRoot;
    }

    public void setReceiptsRoot(HexData receiptsRoot) {
        this.receiptsRoot = receiptsRoot;
    }

    public Address getMiner() {
        return miner;
    }

    public void setMiner(Address miner) {
        this.miner = miner;
    }

    public BigInteger getDifficulty() {
        return difficulty;
    }

    public void setDifficulty(BigInteger difficulty) {
        this.difficulty = difficulty;
    }

    public BigInteger getTotalDifficulty() {
        return totalDifficulty;
    }

    public void setTotalDifficulty(BigInteger totalDifficulty) {
        this.totalDifficulty = totalDifficulty;
    }

    public HexData getExtraData() {
        return Objects.requireNonNullElseGet(extraData, HexData::empty);
    }

    public boolean checkExtraData() {
        return extraData != null;
    }

    public void setExtraData(HexData extraData) {
        this.extraData = extraData;
    }

    public Long getSize() {
        return size;
    }

    public void setSize(Long size) {
        this.size = size;
    }

    public Long getGasLimit() {
        return gasLimit;
    }

    public void setGasLimit(Long gasLimit) {
        this.gasLimit = gasLimit;
    }

    public Long getGasUsed() {
        return gasUsed;
    }

    public void setGasUsed(Long gasUsed) {
        this.gasUsed = gasUsed;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public List<T> getTransactions() {
        return transactions;
    }

    public void setTransactions(List<T> transactions) {
        this.transactions = transactions;
    }

    public List<BlockHash> getUncles() {
        return uncles;
    }

    public void setUncles(List<BlockHash> uncles) {
        this.uncles = uncles;
    }

    public Wei getBaseFeePerGas() {
        return baseFeePerGas;
    }

    public void setBaseFeePerGas(Wei baseFeePerGas) {
        this.baseFeePerGas = baseFeePerGas;
    }

    /**
     * If this instance is empty or contains only references, then return as is. Otherwise
     * returns a copy of the BlockJson with transactions fields replaced with id references
     *
     * @return BlockJson instance with transactions ids only.
     */
    @SuppressWarnings("unchecked")
    public BlockJson<TransactionRefJson> withoutTransactionDetails() {
        // if empty then type doesn't matter
        if (this.transactions == null || this.transactions.isEmpty()) {
            return (BlockJson<TransactionRefJson>) this;
        }
        // if it's already just a reference
        if (this.transactions.stream().noneMatch((tx) -> tx instanceof TransactionJson)) {
            return (BlockJson<TransactionRefJson>) this;
        }
        BlockJson<TransactionRefJson> copy = (BlockJson<TransactionRefJson>) copy();
        copy.transactions = new ArrayList<>(this.transactions.size());
        for (T tx: this.transactions) {
            copy.transactions.add(new TransactionRefJson(tx.getHash()));
        }
        return copy;
    }

    /**
     *
     * @return copy of the current instance
     */
    public BlockJson<T> copy() {
        BlockJson<T> copy = new BlockJson<>();
        copy.number = this.number;
        copy.hash = this.hash;
        copy.parentHash = this.parentHash;
        copy.sha3Uncles = this.sha3Uncles;
        copy.logsBloom = this.logsBloom;
        copy.transactionsRoot = this.transactionsRoot;
        copy.stateRoot = this.stateRoot;
        copy.receiptsRoot = this.receiptsRoot;
        copy.miner = this.miner;
        copy.difficulty = this.difficulty;
        copy.totalDifficulty = this.totalDifficulty;
        copy.extraData = this.extraData;
        copy.size = this.size;
        copy.gasLimit = this.gasLimit;
        copy.gasUsed = this.gasUsed;
        copy.timestamp = this.timestamp;
        copy.transactions = this.transactions;
        copy.uncles = this.uncles;
        copy.baseFeePerGas = this.baseFeePerGas;
        copy.withdrawalsRoot = this.withdrawalsRoot;
        copy.mixHash = this.mixHash;
        copy.nonce = this.nonce;
        return copy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BlockJson<?> blockJson = (BlockJson<?>) o;
        return Objects.equals(number, blockJson.number) && Objects.equals(hash, blockJson.hash) && Objects.equals(parentHash, blockJson.parentHash) && Objects.equals(sha3Uncles, blockJson.sha3Uncles) && Objects.equals(logsBloom, blockJson.logsBloom) && Objects.equals(transactionsRoot, blockJson.transactionsRoot) && Objects.equals(stateRoot, blockJson.stateRoot) && Objects.equals(receiptsRoot, blockJson.receiptsRoot) && Objects.equals(miner, blockJson.miner) && Objects.equals(difficulty, blockJson.difficulty) && Objects.equals(totalDifficulty, blockJson.totalDifficulty) && Objects.equals(extraData, blockJson.extraData) && Objects.equals(size, blockJson.size) && Objects.equals(gasLimit, blockJson.gasLimit) && Objects.equals(gasUsed, blockJson.gasUsed) && Objects.equals(timestamp, blockJson.timestamp) && Objects.equals(transactions, blockJson.transactions) && Objects.equals(uncles, blockJson.uncles) && Objects.equals(baseFeePerGas, blockJson.baseFeePerGas) && Objects.equals(mixHash, blockJson.mixHash) && Objects.equals(nonce, blockJson.nonce) && Objects.equals(withdrawalsRoot, blockJson.withdrawalsRoot);
    }

    @Override
    public int hashCode() {
        return Objects.hash(number, hash, parentHash, sha3Uncles, logsBloom, transactionsRoot, stateRoot, receiptsRoot, miner, difficulty, totalDifficulty, extraData, size, gasLimit, gasUsed, timestamp, transactions, uncles, baseFeePerGas, mixHash, nonce, withdrawalsRoot);
    }
}
