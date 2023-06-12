package io.emeraldpay.dshackle.upstream.ethereum.json;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.emeraldpay.etherjar.domain.BlockHash;
import io.emeraldpay.etherjar.domain.TransactionRef;
import io.emeraldpay.etherjar.domain.Wei;
import io.emeraldpay.etherjar.rpc.json.TransactionRefJson;

import java.io.Serializable;

@JsonDeserialize(using = TransactionJsonSnapshotDeserializer.class)
public class TransactionJsonSnapshot extends TransactionRefJson implements TransactionRef, Serializable {
  /**
   * hash of the block where this transaction was in. null when its pending.
   */
  private BlockHash blockHash;

  /**
   * block number where this transaction was in. null when its pending.
   */
  private Long blockNumber;

  /**
   * gas price provided by the sender in Wei.
   */
  private Wei gasPrice;
  private Wei maxFeePerGas;
  private Wei maxPriorityFeePerGas;

  private int type = 0;

  public int getType() {
    return type;
  }

  public void setType(int type) {
    this.type = type;
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
}
