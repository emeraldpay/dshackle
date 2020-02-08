package io.emeraldpay.dshackle.cache

import io.infinitape.etherjar.domain.BlockHash
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.json.TransactionRefJson
import spock.lang.Specification

class BlockByHeightSpec extends Specification {

    String hash1 = "0xd3f34def3c56ba4e701540d15edaff9acd2a1c968a7ff83b3300ab5dfd5f6aab"
    String hash2 = "0x4aabdaff9acd2f30d15e00ab5dfd5f6c56ba4ea1c968a7ff8d3f34de70153b33"

    def "Fetch with all data available"() {
        setup:
        def blocks = new BlocksMemCache()
        def heights = new HeightCache()

        def block = new BlockJson<TransactionRefJson>()
        block.number = 100
        block.hash = BlockHash.from(hash1)

        blocks.add(block)
        heights.add(block)

        def blocksByHeight = new BlockByHeight(heights, blocks)

        when:
        def act = blocksByHeight.read(100).block()
        then:
        act == block
    }

    def "Fetch correct blocks if multiple"() {
        setup:
        def blocks = new BlocksMemCache()
        def heights = new HeightCache()

        def block1 = new BlockJson<TransactionRefJson>()
        block1.number = 100
        block1.hash = BlockHash.from(hash1)
        def block2 = new BlockJson<TransactionRefJson>()
        block2.number = 101
        block2.hash = BlockHash.from(hash2)

        blocks.add(block1)
        heights.add(block1)
        blocks.add(block2)
        heights.add(block2)

        def blocksByHeight = new BlockByHeight(heights, blocks)

        when:
        def act = blocksByHeight.read(100).block()
        then:
        act == block1

        when:
        act = blocksByHeight.read(101).block()
        then:
        act == block2
    }

    def "Fetch last block if updated"() {
        setup:
        def blocks = new BlocksMemCache()
        def heights = new HeightCache()

        def block1 = new BlockJson<TransactionRefJson>()
        block1.number = 100
        block1.hash = BlockHash.from(hash1)
        def block2 = new BlockJson<TransactionRefJson>()
        block2.number = 100
        block2.hash = BlockHash.from(hash2)

        blocks.add(block1)
        heights.add(block1)
        blocks.add(block2)
        heights.add(block2)

        def blocksByHeight = new BlockByHeight(heights, blocks)

        when:
        def act = blocksByHeight.read(100).block()
        then:
        act == block2
    }

    def "Fetch nothing if block expired"() {
        setup:
        def blocks = new BlocksMemCache()
        def heights = new HeightCache()

        def block = new BlockJson<TransactionRefJson>()
        block.number = 100
        block.hash = BlockHash.from(hash1)

        // add only to heights
        heights.add(block)

        def blocksByHeight = new BlockByHeight(heights, blocks)

        when:
        def act = blocksByHeight.read(100).block()
        then:
        act == null
    }

    def "Fetch nothing if height expired"() {
        setup:
        def blocks = new BlocksMemCache()
        def heights = new HeightCache()

        def block = new BlockJson<TransactionRefJson>()
        block.number = 100
        block.hash = BlockHash.from(hash1)

        // add only to blocks
        blocks.add(block)

        def blocksByHeight = new BlockByHeight(heights, blocks)

        when:
        def act = blocksByHeight.read(100).block()
        then:
        act == null
    }

    def "Fetch nothing if both empty"() {
        setup:
        def blocks = new BlocksMemCache()
        def heights = new HeightCache()
        def blocksByHeight = new BlockByHeight(heights, blocks)
        when:
        def act = blocksByHeight.read(100).block()
        then:
        act == null
    }
}
