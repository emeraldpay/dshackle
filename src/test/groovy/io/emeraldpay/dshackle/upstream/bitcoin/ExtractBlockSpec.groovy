/**
 * Copyright (c) 2020 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.upstream.bitcoin

import io.emeraldpay.dshackle.test.TestingCommons
import spock.lang.Specification

class ExtractBlockSpec extends Specification {

    ExtractBlock extractBlock = new ExtractBlock()

    def "Extract standard block"() {
        setup:
        def json = this.class.getClassLoader().getResourceAsStream("bitcoin/block-626472.json").bytes
        when:
        def act = extractBlock.extract(json)
        then:
        act.hash.toHex() == "0000000000000000000889c2e52ca5e1cecac60bce9a3754201a7a9a67791e90"
        act.height == 626472
        act.timestamp.toString() == "2020-04-18T00:58:46Z"
        act.difficulty.toString(16) == "e9baec5f63190a2b0bbcf6b"
        !act.full
        act.transactions.size() == 1487
        act.json == json
    }

    def "Shouldn't extract time from empty"() {
        when:
        def act = ExtractBlock.getTime([:])
        then:
        act == null
    }

    def "Shouldn't extract time from null"() {
        when:
        def act = ExtractBlock.getTime([time: null])
        then:
        act == null
    }

    def "Extract correct time"() {
        expect:
        ExtractBlock.getTime(block).toString() == time

        where:
        time                   | block
        "2020-04-18T00:58:46Z" | [time: 1587171526]
        "1970-01-01T00:00:00Z" | [time: 0]
    }

    def "Shouldn't extract height from empty"() {
        when:
        def act = ExtractBlock.getHeight([:])
        then:
        act == null
    }

    def "Shouldn't extract height from null"() {
        when:
        def act = ExtractBlock.getHeight([height: null])
        then:
        act == null
    }

    def "Should extract height"() {
        when:
        def act = ExtractBlock.getHeight([height: 123456])
        then:
        act == 123456L
    }

    def "Shouldn't extract difficulty from empty"() {
        when:
        def act = ExtractBlock.getDifficulty([:])
        then:
        act == null
    }

    def "Shouldn't extract difficulty from null"() {
        when:
        def act = ExtractBlock.getDifficulty([chainwork: null])
        then:
        act == null
    }

    def "Should extract difficulty"() {
        when:
        def act = ExtractBlock.getDifficulty([chainwork: "00000000000000000000000000000000000000000ea25fe034fa07aed9e338eb"])
        then:
        act.toString(16) == "ea25fe034fa07aed9e338eb"
    }
}
