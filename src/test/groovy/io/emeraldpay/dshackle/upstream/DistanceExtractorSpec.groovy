package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.etherjar.domain.BlockHash
import io.emeraldpay.dshackle.upstream.ethereum.json.BlockJson
import spock.lang.Specification

import java.time.Instant

class DistanceExtractorSpec extends Specification {
    BlockHash parent = BlockHash.from("0x50d26e119968e791970d84a7bf5d0ec474d3ec2ef85d5ec8915210ac6bc09ad7")

    def "Correct distance for PoW"() {
        expect:
        def top = new BlockJson().with {
            it.number = topHeight
            it.totalDifficulty = topDiff
            it.hash = BlockHash.from("0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915123")
            it.timestamp = Instant.now()
            it.parentHash = parent
            return it
        }
        def curr = new BlockJson().with {
            it.number = currHeight
            it.totalDifficulty = currDiff
            it.hash = BlockHash.from("0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915123")
            it.timestamp = Instant.now()
            it.parentHash = parent
            return it
        }
        delta as DistanceExtractor.ChainDistance == DistanceExtractor.@Companion.extractPowDistance(BlockContainer.from(top), BlockContainer.from(curr))
        where:
        topHeight | topDiff | currHeight | currDiff | delta
        100       | 1000    | 100        | 1000     | new DistanceExtractor.ChainDistance.Distance(0)
        101       | 1010    | 100        | 1000     | new DistanceExtractor.ChainDistance.Distance(1)
        102       | 1020    | 100        | 1000     | new DistanceExtractor.ChainDistance.Distance(2)
        103       | 1030    | 100        | 1000     | new DistanceExtractor.ChainDistance.Distance(3)
        150       | 1500    | 100        | 1000     | new DistanceExtractor.ChainDistance.Distance(50)

        100       | 1000    | 101        | 1010     | new DistanceExtractor.ChainDistance.Distance(0)
        100       | 1000    | 102        | 1020     | new DistanceExtractor.ChainDistance.Distance(0)
        100       | 1000    | 100        | 1010     | DistanceExtractor.ChainDistance.Fork.INSTANCE
        100       | 1100    | 100        | 1000     | DistanceExtractor.ChainDistance.Fork.INSTANCE
    }

    def "Correct distance for priority"() {
        setup:
        def hash1 = "0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915123"
        def hash2 = "0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915124"
        expect:
        def top = new BlockJson().with {
            it.number = topHeight
            it.totalDifficulty = 0
            it.hash = BlockHash.from(hashA == 0 ? hash1 : hash2)
            it.timestamp = Instant.now()
            it.parentHash = parent
            return it
        }
        def curr = new BlockJson().with {
            it.number = currHeight
            it.totalDifficulty = 0
            it.hash = BlockHash.from(hashB == 0 ? hash1 : hash2)
            it.timestamp = Instant.now()
            it.parentHash = top.hash
            return it
        }
        delta as DistanceExtractor.ChainDistance == DistanceExtractor.@Companion.extractPriorityDistance(BlockContainer.from(top), BlockContainer.from(curr))
        where:
        topHeight | hashA | currHeight | hashB || delta
        100       | 0     | 100        | 0     || new DistanceExtractor.ChainDistance.Distance(0)
        101       | 0     | 100        | 1     || new DistanceExtractor.ChainDistance.Distance(1)
        102       | 0     | 100        | 1     || new DistanceExtractor.ChainDistance.Distance(2)
        103       | 0     | 100        | 1     || new DistanceExtractor.ChainDistance.Distance(3)
        150       | 0     | 100        | 1     || new DistanceExtractor.ChainDistance.Distance(50)

        100       | 0     | 101        | 1     || new DistanceExtractor.ChainDistance.Distance(0)
        100       | 0     | 102        | 1     || new DistanceExtractor.ChainDistance.Distance(0)
        100       | 0     | 100        | 1     || DistanceExtractor.ChainDistance.Fork.INSTANCE
    }

    def "Correct distance if parentHash is null"() {
        setup:
        def hash1 = "0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915123"
        def hash2 = "0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915124"
        expect:
        def top = new BlockJson().with {
            it.number = topHeight
            it.totalDifficulty = 0
            it.hash = BlockHash.from(hashA == 0 ? hash1 : hash2)
            it.timestamp = Instant.now()
            it.parentHash = parent
            return it
        }
        def curr = new BlockJson().with {
            it.number = currHeight
            it.totalDifficulty = 0
            it.hash = BlockHash.from(hashB == 0 ? hash1 : hash2)
            it.timestamp = Instant.now()
            it.parentHash = null
            return it
        }
        delta as DistanceExtractor.ChainDistance == DistanceExtractor.@Companion.extractPriorityDistance(BlockContainer.from(top), BlockContainer.from(curr))
        where:
        topHeight | hashA | currHeight | hashB || delta
        105       | 0     | 100        | 0     || new DistanceExtractor.ChainDistance.Distance(5)
        100       | 0     | 101        | 1     || new DistanceExtractor.ChainDistance.Distance(0)
    }
}
