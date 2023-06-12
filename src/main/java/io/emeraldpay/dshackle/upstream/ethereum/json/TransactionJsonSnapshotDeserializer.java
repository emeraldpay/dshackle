package io.emeraldpay.dshackle.upstream.ethereum.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import io.emeraldpay.etherjar.rpc.json.EtherJsonDeserializer;

import java.io.IOException;

public class TransactionJsonSnapshotDeserializer extends EtherJsonDeserializer<TransactionJsonSnapshot> {

  @Override
  public TransactionJsonSnapshot deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
    JsonNode node = jp.readValueAsTree();
    return deserialize(node);
  }

  public TransactionJsonSnapshot deserialize(JsonNode node) {
    TransactionJsonSnapshot tx = new TransactionJsonSnapshot();
    tx.setHash(getTxHash(node, "hash"));
    tx.setBlockHash(getBlockHash(node, "blockHash"));
    Long blockNumber = getLong(node, "blockNumber");
    if (blockNumber != null)  {
      tx.setBlockNumber(blockNumber);
    }
    Integer type = getInt(node, "type");
    if (type != null) {
      tx.setType(type);
    }
    tx.setGasPrice(getWei(node, "gasPrice"));
    tx.setMaxFeePerGas(getWei(node, "maxFeePerGas"));
    tx.setMaxPriorityFeePerGas(getWei(node, "maxPriorityFeePerGas"));

    return tx;
  }
}
