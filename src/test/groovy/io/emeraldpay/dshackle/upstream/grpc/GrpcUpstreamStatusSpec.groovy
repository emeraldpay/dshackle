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
package io.emeraldpay.dshackle.upstream.grpc

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.startup.QuorumForLabels
import io.emeraldpay.dshackle.upstream.calls.DirectCallMethods
import spock.lang.Specification

class GrpcUpstreamStatusSpec extends Specification {

    def "Updates with new labels"() {
        setup:
        def status = new GrpcUpstreamStatus(null)
        when:
        status.update(
                BlockchainOuterClass.DescribeChain.newBuilder()
                        .addNodes(
                                BlockchainOuterClass.NodeDetails.newBuilder()
                                        .setQuorum(1)
                                        .addLabels(
                                                BlockchainOuterClass.Label.newBuilder().setName("test").setValue("foo")
                                        )
                        )
                        .build()
        )
        def act = status.getLabels()
        then:
        act.toList() == [
                UpstreamsConfig.Labels.fromMap([test: "foo"])
        ]

        // replace with new value
        when:
        status.update(
                BlockchainOuterClass.DescribeChain.newBuilder()
                        .addNodes(
                                BlockchainOuterClass.NodeDetails.newBuilder()
                                        .setQuorum(1)
                                        .addLabels(
                                                BlockchainOuterClass.Label.newBuilder().setName("test").setValue("bar")
                                        )
                        )
                        .build()
        )
        act = status.getLabels()
        then:
        act.toList() == [
                UpstreamsConfig.Labels.fromMap([test: "bar"])
        ]

        // more values
        when:
        def result = status.update(
                BlockchainOuterClass.DescribeChain.newBuilder()
                        .addNodes(
                                BlockchainOuterClass.NodeDetails.newBuilder()
                                        .setQuorum(1)
                                        .addLabels(
                                                BlockchainOuterClass.Label.newBuilder().setName("test1").setValue("bar")
                                        )
                                        .addLabels(
                                                BlockchainOuterClass.Label.newBuilder().setName("test2").setValue("baz")
                                        )
                        )
                        .build()
        )
        act = status.getLabels()
        then:
        result
        act.toList() == [
                UpstreamsConfig.Labels.fromMap([test1: "bar", test2: "baz"])
        ]
    }

    def "Updates with new labels override"() {
        setup:
        def status = new GrpcUpstreamStatus(UpstreamsConfig.Labels.fromMap([fix: "value"]))
        when:
        status.update(
                BlockchainOuterClass.DescribeChain.newBuilder()
                        .addNodes(
                                BlockchainOuterClass.NodeDetails.newBuilder()
                                        .setQuorum(1)
                                        .addLabels(
                                                BlockchainOuterClass.Label.newBuilder().setName("test").setValue("foo")
                                        )
                        )
                        .build()
        )
        def act = status.getLabels()
        then:
        act.toList() == [
                UpstreamsConfig.Labels.fromMap([test: "foo", fix: "value"])
        ]

        // replace with new value
        when:
        def result = status.update(
                BlockchainOuterClass.DescribeChain.newBuilder()
                        .addNodes(
                                BlockchainOuterClass.NodeDetails.newBuilder()
                                        .setQuorum(1)
                                        .addLabels(
                                                BlockchainOuterClass.Label.newBuilder().setName("test").setValue("bar")
                                        ).addLabels(
                                        BlockchainOuterClass.Label.newBuilder().setName("fix").setValue("val")
                                )
                        )
                        .build()
        )
        act = status.getLabels()
        then:
        result
        act.toList() == [
                UpstreamsConfig.Labels.fromMap([test: "bar", fix: "value"])
        ]

        // more values
        when:
        status.update(
                BlockchainOuterClass.DescribeChain.newBuilder()
                        .addNodes(
                                BlockchainOuterClass.NodeDetails.newBuilder()
                                        .setQuorum(1)
                                        .addLabels(
                                                BlockchainOuterClass.Label.newBuilder().setName("test1").setValue("bar")
                                        )
                                        .addLabels(
                                                BlockchainOuterClass.Label.newBuilder().setName("test2").setValue("baz")
                                        )
                        )
                        .build()
        )
        act = status.getLabels()
        then:
        act.toList() == [
                UpstreamsConfig.Labels.fromMap([test1: "bar", test2: "baz", fix: "value"])
        ]
    }

    def "Updates with new nodes"() {
        setup:
        def status = new GrpcUpstreamStatus(null)
        when:
        status.update(
                BlockchainOuterClass.DescribeChain.newBuilder()
                        .addNodes(
                                BlockchainOuterClass.NodeDetails.newBuilder()
                                        .setQuorum(1)
                                        .addLabels(
                                                BlockchainOuterClass.Label.newBuilder().setName("test").setValue("foo")
                                        )
                        )
                        .build()
        )
        def act = status.getNodes()
        then:
        act == new QuorumForLabels().tap {
            it.add(new QuorumForLabels.QuorumItem(1, UpstreamsConfig.Labels.fromMap([test: "foo"])))
        }
    }

    def "Updates with new nodes override labels"() {
        setup:
        def status = new GrpcUpstreamStatus(UpstreamsConfig.Labels.fromMap([fix: "value"]))
        when:
        status.update(
                BlockchainOuterClass.DescribeChain.newBuilder()
                        .addNodes(
                                BlockchainOuterClass.NodeDetails.newBuilder()
                                        .setQuorum(1)
                                        .addLabels(
                                                BlockchainOuterClass.Label.newBuilder().setName("test").setValue("foo")
                                        )
                                        .addLabels(
                                                BlockchainOuterClass.Label.newBuilder().setName("fix").setValue("val")
                                        )
                        )
                        .build()
        )
        def act = status.getNodes()
        then:
        act == new QuorumForLabels().tap {
            it.add(new QuorumForLabels.QuorumItem(1, UpstreamsConfig.Labels.fromMap([test: "foo", fix: "value"])))
        }
    }

    def "Updates with methods"() {
        setup:
        def status = new GrpcUpstreamStatus(null)
        when:
        def result = status.update(
                BlockchainOuterClass.DescribeChain.newBuilder()
                        .addAllSupportedMethods([
                                "test_1",
                                "test_2"
                        ])
                        .build()
        )
        def act = status.getCallMethods()
        then:
        result
        act.supportedMethods == ["test_1", "test_2"].toSet()
        act instanceof DirectCallMethods
    }

    def "Updates with new methods"() {
        setup:
        def status = new GrpcUpstreamStatus(null)
        status.update(
                BlockchainOuterClass.DescribeChain.newBuilder()
                        .addAllSupportedMethods([
                                "test_1",
                                "test_2"
                        ])
                        .build()
        )
        when:
        def result = status.update(
                BlockchainOuterClass.DescribeChain.newBuilder()
                        .addAllSupportedMethods([
                                "test_1",
                                "test_2",
                                "test_3"
                        ])
                        .build())
        def act = status.getCallMethods()
        then:
        result
        act.supportedMethods == ["test_1", "test_2", "test_3"].toSet()
        act instanceof DirectCallMethods
    }
}
