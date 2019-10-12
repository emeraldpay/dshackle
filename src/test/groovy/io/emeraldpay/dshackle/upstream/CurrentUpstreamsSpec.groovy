package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.test.EthereumUpstreamMock
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.rpc.ReactorRpcClient
import spock.lang.Specification

class CurrentUpstreamsSpec extends Specification {

    def "add upstream"() {
        setup:
        def current = new CurrentUpstreams(TestingCommons.objectMapper())
        def up = new EthereumUpstreamMock("test", Chain.ETHEREUM, TestingCommons.api(Stub(ReactorRpcClient)))
        when:
        current.update(new UpstreamChange(Chain.ETHEREUM, up, UpstreamChange.ChangeType.ADDED))
        then:
        current.getAvailable() == [Chain.ETHEREUM]
        current.getUpstream(Chain.ETHEREUM).getAll()[0] == up
    }

    def "add multiple upstreams"() {
        setup:
        def current = new CurrentUpstreams(TestingCommons.objectMapper())
        def up1 = new EthereumUpstreamMock("test1", Chain.ETHEREUM, TestingCommons.api(Stub(ReactorRpcClient)))
        def up2 = new EthereumUpstreamMock("test2", Chain.ETHEREUM_CLASSIC, TestingCommons.api(Stub(ReactorRpcClient)))
        def up3 = new EthereumUpstreamMock("test3", Chain.ETHEREUM, TestingCommons.api(Stub(ReactorRpcClient)))
        when:
        current.update(new UpstreamChange(Chain.ETHEREUM, up1, UpstreamChange.ChangeType.ADDED))
        current.update(new UpstreamChange(Chain.ETHEREUM_CLASSIC, up2, UpstreamChange.ChangeType.ADDED))
        current.update(new UpstreamChange(Chain.ETHEREUM, up3, UpstreamChange.ChangeType.ADDED))
        then:
        current.getAvailable().toSet() == [Chain.ETHEREUM, Chain.ETHEREUM_CLASSIC].toSet()
        current.getUpstream(Chain.ETHEREUM).getAll().toSet() == [up1, up3].toSet()
        current.getUpstream(Chain.ETHEREUM_CLASSIC).getAll().toSet() == [up2].toSet()
    }

    def "remove upstream"() {
        setup:
        def current = new CurrentUpstreams(TestingCommons.objectMapper())
        def up1 = new EthereumUpstreamMock("test1", Chain.ETHEREUM, TestingCommons.api(Stub(ReactorRpcClient)))
        def up2 = new EthereumUpstreamMock("test2", Chain.ETHEREUM_CLASSIC, TestingCommons.api(Stub(ReactorRpcClient)))
        def up3 = new EthereumUpstreamMock("test3", Chain.ETHEREUM, TestingCommons.api(Stub(ReactorRpcClient)))
        def up1_del = new EthereumUpstreamMock("test1", Chain.ETHEREUM, TestingCommons.api(Stub(ReactorRpcClient)))
        when:
        current.update(new UpstreamChange(Chain.ETHEREUM, up1, UpstreamChange.ChangeType.ADDED))
        current.update(new UpstreamChange(Chain.ETHEREUM_CLASSIC, up2, UpstreamChange.ChangeType.ADDED))
        current.update(new UpstreamChange(Chain.ETHEREUM, up3, UpstreamChange.ChangeType.ADDED))
        current.update(new UpstreamChange(Chain.ETHEREUM, up1_del, UpstreamChange.ChangeType.REMOVED))
        then:
        current.getAvailable().toSet() == [Chain.ETHEREUM, Chain.ETHEREUM_CLASSIC].toSet()
        current.getUpstream(Chain.ETHEREUM).getAll().toSet() == [up3].toSet()
        current.getUpstream(Chain.ETHEREUM_CLASSIC).getAll().toSet() == [up2].toSet()
    }
}
