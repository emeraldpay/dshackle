package io.emeraldpay.dshackle.upstream.ethereum.domain;

/**
 * Transaction reference
 */
public interface TransactionRef {

    /**
     *
     * @return hash of the transaction
     */
    TransactionId getHash();

}
