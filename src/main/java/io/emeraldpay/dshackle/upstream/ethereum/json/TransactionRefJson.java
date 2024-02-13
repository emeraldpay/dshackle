package io.emeraldpay.dshackle.upstream.ethereum.json;

import io.emeraldpay.dshackle.upstream.ethereum.domain.TransactionId;
import io.emeraldpay.dshackle.upstream.ethereum.domain.TransactionRef;

import java.util.Objects;

/**
 * A simple reference to a transaction
 */
public class TransactionRefJson implements TransactionRef {

    private TransactionId hash;

    public TransactionRefJson() {
    }

    public TransactionRefJson(TransactionId hash) {
        this.hash = hash;
    }

    @Override
    public TransactionId getHash() {
        return hash;
    }

    public void setHash(TransactionId hash) {
        this.hash = hash;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransactionRefJson that = (TransactionRefJson) o;
        return Objects.equals(hash, that.hash);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hash);
    }
}
