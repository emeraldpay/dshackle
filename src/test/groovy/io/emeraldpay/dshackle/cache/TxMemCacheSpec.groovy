package io.emeraldpay.dshackle.cache

import io.infinitape.etherjar.domain.BlockHash
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.json.TransactionJson
import io.infinitape.etherjar.rpc.json.TransactionRefJson
import spock.lang.Specification

class TxMemCacheSpec extends Specification {

    String hash1 = "0xd3f34def3c56ba4e701540d15edaff9acd2a1c968a7ff83b3300ab5dfd5f6aab"
    String hash2 = "0x4aabdaff9acd2f30d15e00ab5dfd5f6c56ba4ea1c968a7ff8d3f34de70153b33"
    String hash3 = "0x40d15edaff9acdabd2a1c96fd5f683b3300aad34e7015f34def3c56ba8a7ffb5"
    String hash4 = "0xa4e7a75dfd5f6a83b3304dc56bfa0abfd3fef01540d15edafc9683f9acd2a13b"

    def "Add and read"() {
        setup:
        def cache = new TxMemCache()
        def tx = new TransactionJson()
        tx.hash = TransactionId.from(hash1)
        tx.blockHash = BlockHash.from(hash1)
        tx.blockNumber = 100

        when:
        cache.add(tx)
        def act = cache.read(TransactionId.from(hash1)).block()
        then:
        act == tx
    }

    def "Keeps only configured amount"() {
        setup:
        def cache = new TxMemCache(3)

        when:
        [hash1, hash2, hash3, hash4].eachWithIndex{ String hash, int i ->
            def tx = new TransactionJson()
            tx.blockNumber = 100 + i
            tx.blockHash = BlockHash.from(hash)
            tx.hash = TransactionId.from(hash)
            cache.add(tx)
        }

        def act1 = cache.read(TransactionId.from(hash1)).block()
        def act2 = cache.read(TransactionId.from(hash2)).block()
        def act3 = cache.read(TransactionId.from(hash3)).block()
        def act4 = cache.read(TransactionId.from(hash4)).block()
        then:
        act2.hash.toHex() == hash2
        act3.hash.toHex() == hash3
        act4.hash.toHex() == hash4
        act1 == null
    }

    def "Evict all by block hash"() {
        setup:
        def cache = new TxMemCache()

        when:
        [hash1, hash2].eachWithIndex{ String hash, int i ->
            def tx = new TransactionJson()
            tx.blockNumber = 100
            tx.blockHash = BlockHash.from(hash1)
            tx.hash = TransactionId.from(hash)
            cache.add(tx)
        }
        [hash3, hash4].eachWithIndex{ String hash, int i ->
            def tx = new TransactionJson()
            tx.blockNumber = 101
            tx.blockHash = BlockHash.from(hash2)
            tx.hash = TransactionId.from(hash)
            cache.add(tx)
        }

        cache.evict(BlockHash.from(hash1))

        def act1 = cache.read(TransactionId.from(hash1)).block()
        def act2 = cache.read(TransactionId.from(hash2)).block()
        def act3 = cache.read(TransactionId.from(hash3)).block()
        def act4 = cache.read(TransactionId.from(hash4)).block()

        then:
        act1 == null
        act2 == null
        act3.hash.toHex() == hash3
        act4.hash.toHex() == hash4
    }

    def "Evict all by block data"() {
        setup:
        def cache = new TxMemCache()

        when:
        [hash1, hash2].eachWithIndex{ String hash, int i ->
            def tx = new TransactionJson()
            tx.blockNumber = 100
            tx.blockHash = BlockHash.from(hash1)
            tx.hash = TransactionId.from(hash)
            cache.add(tx)
        }
        [hash3, hash4].eachWithIndex{ String hash, int i ->
            def tx = new TransactionJson()
            tx.blockNumber = 100
            tx.blockHash = BlockHash.from(hash2)
            tx.hash = TransactionId.from(hash)
            cache.add(tx)
        }

        def block = new BlockJson<TransactionRefJson>()
        block.hash = BlockHash.from(hash1)
        block.number = 100
        block.transactions = [
                new TransactionRefJson(TransactionId.from(hash1)),
                new TransactionRefJson(TransactionId.from(hash2)),
        ]

        cache.evict(block)

        def act1 = cache.read(TransactionId.from(hash1)).block()
        def act2 = cache.read(TransactionId.from(hash2)).block()
        def act3 = cache.read(TransactionId.from(hash3)).block()
        def act4 = cache.read(TransactionId.from(hash4)).block()

        then:
        act1 == null
        act2 == null
        act3.hash.toHex() == hash3
        act4.hash.toHex() == hash4
    }
}
