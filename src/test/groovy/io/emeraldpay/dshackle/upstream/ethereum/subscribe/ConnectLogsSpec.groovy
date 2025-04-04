/**
 * Copyright (c) 2021 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.upstream.ethereum.subscribe

import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.json.LogMessage
import io.emeraldpay.dshackle.upstream.ethereum.domain.Address
import io.emeraldpay.dshackle.upstream.ethereum.domain.BlockHash
import io.emeraldpay.dshackle.upstream.ethereum.domain.TransactionId
import io.emeraldpay.dshackle.upstream.ethereum.hex.Hex32
import io.emeraldpay.dshackle.upstream.ethereum.hex.HexData
import reactor.core.publisher.Flux
import reactor.core.scheduler.Schedulers
import spock.lang.Specification

class ConnectLogsSpec extends Specification {

    def log1 = new LogMessage(
            Address.from("0x298d492e8c1d909d3f63bc4a36c66c64acb3d695"),
            BlockHash.empty(),
            100L,
            HexData.empty(),
            1L,
            [
                    Hex32.from("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
            ],
            TransactionId.from("0xb5e554178a94fd993111f2ae64cb708cb0899d7b5182024e70d5c468164a8bec"),
            1L,
            false,
            "upstream"
    )

    def log2 = new LogMessage(
            Address.from("0x63bc4a36c66c64acb3d695298d492e8c1d909d3f"),
            BlockHash.empty(),
            100L,
            HexData.empty(),
            1L,
            [
                    Hex32.from("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
            ],
            TransactionId.from("0xb5e554178a94fd993111f2ae64cb708cb0899d7b5182024e70d5c468164a8bec"),
            1L,
            false,
            "upstream"
    )

    def log3 = new LogMessage(
            Address.from("0x63bc4a36c66c64acb3d695298d492e8c1d909d3f"),
            BlockHash.empty(),
            100L,
            HexData.empty(),
            1L,
            [
                    Hex32.from("0x952ba7f163c4a11628f55a4df523b3efddf252ad1be2c89b69c2b068fc378daa")
            ],
            TransactionId.from("0xb5e554178a94fd993111f2ae64cb708cb0899d7b5182024e70d5c468164a8bec"),
            1L,
            false,
            "upstream"
    )

    def log4 = new LogMessage(
            Address.from("0x4a36c66c64acb3d695298d492e8c1d909d3f63bc"),
            BlockHash.empty(),
            100L,
            HexData.empty(),
            1L,
            [
                    Hex32.from("0x952ba7f163c4a11628f55a4df523b3efddf252ad1be2c89b69c2b068fc378daa")
            ],
            TransactionId.from("0xb5e554178a94fd993111f2ae64cb708cb0899d7b5182024e70d5c468164a8bec"),
            1L,
            false,
            "upstream"
    )

    def log5 = new LogMessage(
            Address.from("0x63bc4a36c66c64acb3d695298d492e8c1d909d3f"),
            BlockHash.empty(),
            100L,
            HexData.empty(),
            1L,
            [
                    Hex32.from("0x952ba7f163c4a11628f55a4df523b3efddf252ad1be2c89b69c2b068fc378daa"),
                    Hex32.from("0x00000000000000000000000088e6a0c2ddd26feeb64f039a2c41296fcb3f5640")
            ],
            TransactionId.from("0xb5e554178a94fd993111f2ae64cb708cb0899d7b5182024e70d5c468164a8bec"),
            1L,
            false,
            "upstream"
    )

    def log6 = new LogMessage(
            Address.from("0x63bc4a36c66c64acb3d695298d492e8c1d909d3f"),
            BlockHash.empty(),
            100L,
            HexData.empty(),
            1L,
            [
                    Hex32.from("0x952ba7f163c4a11628f55a4df523b3efddf252ad1be2c89b69c2b068fc378daa"),
                    Hex32.from("0x00000000000000000000000088e6a0c2ddd26feeb64f039a2c41296fcb3f5640"),
                    Hex32.from("0x00000000000000000000000088e6a0c2ddd26feeb64f039a2c41296fcb3f5641")
            ],
            TransactionId.from("0xb5e554178a94fd993111f2ae64cb708cb0899d7b5182024e70d5c468164a8bec"),
            1L,
            false,
            "upstream"
    )

    def "Filter is empty"() {
        setup:
        def connectLogs = new ConnectLogs(TestingCommons.emptyMultistream(), Schedulers.boundedElastic())
        when:
        def input = Flux.fromIterable([
                log1, log2, log3, log4
        ])
        def act = input.transform(connectLogs.filtered([], []))
                .collectList().block()

        then:
        act.size() == 4
        act[0] == log1
        act[1] == log2
        act[2] == log3
        act[3] == log4
    }

    def "Filter by address"() {
        setup:
        def connectLogs = new ConnectLogs(TestingCommons.emptyMultistream(), Schedulers.boundedElastic())
        when:
        def input = Flux.fromIterable([
                log1, log2
        ])
        def act = input.transform(connectLogs.filtered([Address.from("0x298d492e8c1d909d3f63bc4a36c66c64acb3d695")], []))
                .collectList().block()

        then:
        act.size() == 1
        act[0] == log1
    }

    def "Filter by topic"() {
        setup:
        def connectLogs = new ConnectLogs(TestingCommons.emptyMultistream(), Schedulers.boundedElastic())
        when:
        def input = Flux.fromIterable([
                log1, log2, log3, log4
        ])
        def act = input.transform(connectLogs.filtered([], [Hex32.from("0x952ba7f163c4a11628f55a4df523b3efddf252ad1be2c89b69c2b068fc378daa")]))
                .collectList().block()

        then:
        act.size() == 2
        act[0] == log3
        act[1] == log4
    }


    def "Filter by address and topic"() {
        setup:
        def connectLogs = new ConnectLogs(TestingCommons.emptyMultistream(), Schedulers.boundedElastic())
        when:
        def input = Flux.fromIterable([
                log1, log2, log3, log4
        ])
        def act = input.transform(connectLogs.filtered([Address.from("0x63bc4a36c66c64acb3d695298d492e8c1d909d3f")], [Hex32.from("0x952ba7f163c4a11628f55a4df523b3efddf252ad1be2c89b69c2b068fc378daa")]))
                .collectList().block()

        then:
        act.size() == 1
        act[0] == log3
    }

    def "Filter by address and two topics"() {
        setup:
        def connectLogs = new ConnectLogs(TestingCommons.emptyMultistream(), Schedulers.boundedElastic())
        when:
        def input = Flux.fromIterable([
                log1, log2, log3, log4, log5, log6
        ])

        def act = input.transform(connectLogs.filtered(
                [Address.from("0x63bc4a36c66c64acb3d695298d492e8c1d909d3f")],
                [
                        Hex32.from("0x952ba7f163c4a11628f55a4df523b3efddf252ad1be2c89b69c2b068fc378daa"),
                        Hex32.from("0x00000000000000000000000088e6a0c2ddd26feeb64f039a2c41296fcb3f5640"),
                ]
        ))
                .collectList().block()

        then:
        act.size() == 2
        act[0] == log5
        act[1] == log6
    }

    def "Filter by address and second topics"() {
        setup:
        def connectLogs = new ConnectLogs(TestingCommons.emptyMultistream(), Schedulers.boundedElastic())
        when:
        def input = Flux.fromIterable([
                log1, log2, log3, log4, log5, log6
        ])

        def act = input.transform(connectLogs.filtered(
                [Address.from("0x63bc4a36c66c64acb3d695298d492e8c1d909d3f")],
                [
                        Hex32.from("0x952ba7f163c4a11628f55a4df523b3efddf252ad1be2c89b69c2b068fc378daa"),
                        null,
                        Hex32.from("0x00000000000000000000000088e6a0c2ddd26feeb64f039a2c41296fcb3f5641"),
                ]
        ))
                .collectList().block()

        then:
        act.size() == 1
        act[0] == log6
    }
}