/**
 * Copyright (c) 2022 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.etherjar.domain.BlockHash
import io.emeraldpay.etherjar.hex.Hex32
import spock.lang.Specification

class PriorityForkChoiceSpec extends Specification {

    def "Gets the best preferred upstream"() {
        setup:
        def up100 = TestingCommons.upstream("up-100")
        def up20 = TestingCommons.upstream("up-20")
        def up3 = TestingCommons.upstream("up-3")
        when:
        def forkChoice = new PriorityForkChoice()
        forkChoice.addUpstream(up100, 100)
        forkChoice.addUpstream(up20, 20)
        forkChoice.addUpstream(up3, 3)

        def act = forkChoice.findPreferentialUpstream(up3)
        then:
        act == up100

        when:
        act = forkChoice.findPreferentialUpstream(up20)
        then:
        act == up100
    }

    def "Gets the best preferred upstream which must be available"() {
        setup:
        def up100 = TestingCommons.upstream("up-100").tap {
            mockStatus = UpstreamAvailability.SYNCING
        }
        def up20 = TestingCommons.upstream("up-20")
        def up3 = TestingCommons.upstream("up-3")
        when:
        def forkChoice = new PriorityForkChoice()
        forkChoice.addUpstream(up100, 100)
        forkChoice.addUpstream(up20, 20)
        forkChoice.addUpstream(up3, 3)

        def act = forkChoice.findPreferentialUpstream(up3)
        then:
        act == up20

        when:
        act = forkChoice.findPreferentialUpstream(up20)
        then:
        act == null
    }

    def "No preferred upstream when itself is the best"() {
        setup:
        def up100 = TestingCommons.upstream("up-100")
        def up20 = TestingCommons.upstream("up-20")
        def up3 = TestingCommons.upstream("up-3")
        when:
        def forkChoice = new PriorityForkChoice()
        forkChoice.addUpstream(up100, 100)
        forkChoice.addUpstream(up20, 20)
        forkChoice.addUpstream(up3, 3)

        def act = forkChoice.findPreferentialUpstream(up100)
        then:
        act == null
    }

    def "Finds parent in journal"() {
        setup:
        def forkChoice = new PriorityForkChoice()
        forkChoice.keepJournal(BlockId.from("aa01"), BlockId.from("aa02"))
        forkChoice.keepJournal(BlockId.from("aa02"), BlockId.from("aa03"))
        forkChoice.keepJournal(BlockId.from("aa02"), BlockId.from("bb03"))
        forkChoice.keepJournal(BlockId.from("bb03"), BlockId.from("bb04"))
        forkChoice.keepJournal(BlockId.from("aa03"), BlockId.from("aa04"))

        expect:
        def act = forkChoice.findParentInJournal(BlockId.from(block))
        (parent == null && act == null) || (act.first == BlockId.from(parent) && act.second == BlockId.from(block))

        where:
        parent  | block
        null    | "aa01"
        "aa01"  | "aa02"
        "aa02"  | "aa03"
        "aa03"  | "aa04"

        null    | "bb01"
        null    | "bb02"
        "aa02"  | "bb03"
        "bb03"  | "bb04"

        null    | "cc05"
    }

    def "Rebuild blocks when adds a new on top"() {
        setup:
        def forkChoice = new PriorityForkChoice()
        def existing = [BlockId.from("01"), BlockId.from("02")]
        when:
        def act = forkChoice.rebuild(existing, BlockId.from("02"), BlockId.from("03"))
        then:
        act == [BlockId.from("01"), BlockId.from("02"), BlockId.from("03")]
    }

    def "Rebuild blocks when existing is empty"() {
        setup:
        def forkChoice = new PriorityForkChoice()
        def existing = []
        when:
        def act = forkChoice.rebuild(existing, BlockId.from("02"), BlockId.from("03"))
        then:
        act == [BlockId.from("02"), BlockId.from("03")]
    }

    def "Rebuild missing block using journal"() {
        setup:
        def forkChoice = new PriorityForkChoice()
        forkChoice.keepJournal(BlockId.from("01"), BlockId.from("02"))
        forkChoice.keepJournal(BlockId.from("02"), BlockId.from("03"))
        def existing = [BlockId.from("01"), BlockId.from("02")]
        when:
        def act = forkChoice.rebuild(existing, BlockId.from("03"), BlockId.from("04"))
        then:
        act == [BlockId.from("01"), BlockId.from("02"), BlockId.from("03"), BlockId.from("04")]
    }

    def "Rebuild few missing blocks using journal"() {
        setup:
        def forkChoice = new PriorityForkChoice()
        forkChoice.keepJournal(BlockId.from("01"), BlockId.from("02"))
        forkChoice.keepJournal(BlockId.from("02"), BlockId.from("03"))
        forkChoice.keepJournal(BlockId.from("03"), BlockId.from("04"))
        forkChoice.keepJournal(BlockId.from("04"), BlockId.from("05"))
        def existing = [BlockId.from("01"), BlockId.from("02")]
        when:
        def act = forkChoice.rebuild(existing, BlockId.from("04"), BlockId.from("05"))
        then:
        act == [BlockId.from("01"), BlockId.from("02"), BlockId.from("03"), BlockId.from("04"), BlockId.from("05")]
    }

    def "Merge on top"() {
        setup:
        def forkChoice = new PriorityForkChoice()
        def existing = [BlockId.from("01"), BlockId.from("02")]
        when:
        def act = forkChoice.merge(existing, BlockId.from("02"), BlockId.from("03"))
        then:
        act == [BlockId.from("01"), BlockId.from("02"), BlockId.from("03")]
    }

    def "Merge with missing block"() {
        setup:
        def forkChoice = new PriorityForkChoice()
        forkChoice.keepJournal(BlockId.from("01"), BlockId.from("02"))
        forkChoice.keepJournal(BlockId.from("02"), BlockId.from("03"))
        def existing = [BlockId.from("01"), BlockId.from("02")]
        when:
        def act = forkChoice.merge(existing, BlockId.from("03"), BlockId.from("04"))
        then:
        act == [BlockId.from("01"), BlockId.from("02"), BlockId.from("03"), BlockId.from("04")]
    }

    def "Merge with forked block"() {
        setup:
        def forkChoice = new PriorityForkChoice()
        def existing = [BlockId.from("aa01"), BlockId.from("aa02"), BlockId.from("aa03")]
        when:
        def act = forkChoice.merge(existing, BlockId.from("aa02"), BlockId.from("bb03"))
        then:
        act == [BlockId.from("aa01"), BlockId.from("aa02"), BlockId.from("bb03")]
    }

    def "Compare as NEW if nothing recognized"() {
        setup:
        def forkChoice = new PriorityForkChoice()
        def recognized = []
        def current = [BlockId.from("03"), BlockId.from("04")]
        when:
        def act = forkChoice.compareHistory(recognized, current)
        then:
        act == ForkChoice.Status.NEW
    }

    def "Compare as OUTRUN if follows recognized"() {
        setup:
        def forkChoice = new PriorityForkChoice()
        def recognized = [BlockId.from("01"), BlockId.from("02")]
        def current = [BlockId.from("02"), BlockId.from("03")]
        when:
        def act = forkChoice.compareHistory(recognized, current)
        then:
        act == ForkChoice.Status.OUTRUN
    }

    def "Compare as OUTRUN if follows recognized on long"() {
        setup:
        def forkChoice = new PriorityForkChoice()
        def recognized = [BlockId.from("01"), BlockId.from("02")]
        def current = [BlockId.from("02"), BlockId.from("03"), BlockId.from("04"), BlockId.from("05")]
        when:
        def act = forkChoice.compareHistory(recognized, current)
        then:
        act == ForkChoice.Status.OUTRUN
    }

    def "Compare as EQUAL if the same as recognized"() {
        setup:
        def forkChoice = new PriorityForkChoice()
        def recognized = [BlockId.from("01"), BlockId.from("02")]
        def current = [BlockId.from("01"), BlockId.from("02")]
        when:
        def act = forkChoice.compareHistory(recognized, current)
        then:
        act == ForkChoice.Status.EQUAL
    }

    def "Compare as EQUAL if the same as recognized even if shorter"() {
        setup:
        def forkChoice = new PriorityForkChoice()
        def recognized = [BlockId.from("01"), BlockId.from("02"), BlockId.from("03")]
        def current = [BlockId.from("02"), BlockId.from("03")]
        when:
        def act = forkChoice.compareHistory(recognized, current)
        then:
        act == ForkChoice.Status.EQUAL
    }

    def "Compare as REJECTED if forked"() {
        setup:
        def forkChoice = new PriorityForkChoice()
        def recognized = [BlockId.from("aa01"), BlockId.from("aa02"), BlockId.from("aa03")]
        def current = [BlockId.from("aa02"), BlockId.from("bb03")]
        when:
        def act = forkChoice.compareHistory(recognized, current)
        then:
        act == ForkChoice.Status.REJECTED
    }

    def "Compare as REJECTED if no history"() {
        setup:
        def forkChoice = new PriorityForkChoice()
        def recognized = [BlockId.from("aa01"), BlockId.from("aa02"), BlockId.from("aa03")]
        def current = []
        when:
        def act = forkChoice.compareHistory(recognized, current)
        then:
        act == ForkChoice.Status.REJECTED
    }

    def "Submit from most preferred upstream"() {
        setup:
        def up100 = TestingCommons.upstream("up-100")
        def up20 = TestingCommons.upstream("up-20")
        def up3 = TestingCommons.upstream("up-3")
        when:
        def forkChoice = new PriorityForkChoice()
        forkChoice.addUpstream(up100, 100)
        forkChoice.addUpstream(up20, 20)
        forkChoice.addUpstream(up3, 3)

        def act = forkChoice.submit(
                TestingCommons.blockForEthereum(100),
                up100
        )
        then:
        act == ForkChoice.Status.NEW

        when:
        act = forkChoice.submit(
                TestingCommons.blockForEthereum(101),
                up100
        )
        then:
        act == ForkChoice.Status.NEW

        when:
        act = forkChoice.submit(
                // not that the block is on the same height, but since it's from the preferred upstreams it must be considered as a new block
                TestingCommons.blockForEthereum(101),
                up100
        )
        then:
        act == ForkChoice.Status.NEW
    }

    def "Submit from not preferred is ok if same data"() {
        setup:
        def up100 = TestingCommons.upstream("up-100")
        def up20 = TestingCommons.upstream("up-20")
        def up3 = TestingCommons.upstream("up-3")
        when:
        def forkChoice = new PriorityForkChoice()
        forkChoice.addUpstream(up100, 100)
        forkChoice.addUpstream(up20, 20)
        forkChoice.addUpstream(up3, 3)

        def act = forkChoice.submit(
                TestingCommons.blockForEthereum(100),
                up100
        )
        then:
        act == ForkChoice.Status.NEW

        when:
        act = forkChoice.submit(
                TestingCommons.blockForEthereum(100),
                up20
        )
        then:
        act == ForkChoice.Status.EQUAL
    }

    def "Submit from not preferred crumbles if different block"() {
        setup:
        def up100 = TestingCommons.upstream("up-100")
        def up20 = TestingCommons.upstream("up-20")
        def up3 = TestingCommons.upstream("up-3")
        when:
        def forkChoice = new PriorityForkChoice()
        forkChoice.addUpstream(up100, 100)
        forkChoice.addUpstream(up20, 20)
        forkChoice.addUpstream(up3, 3)

        def act = forkChoice.submit(
                TestingCommons.blockForEthereum(100),
                up100
        )
        then:
        act == ForkChoice.Status.NEW

        when:
        act = forkChoice.submit(
                TestingCommons.blockForEthereum(100, BlockHash.from(Hex32.extendFrom(1000).bytes)),
                up20
        )
        then:
        act == ForkChoice.Status.REJECTED
    }

}
