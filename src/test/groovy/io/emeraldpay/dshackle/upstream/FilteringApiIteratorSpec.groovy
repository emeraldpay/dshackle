package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.rpc.DefaultRpcClient
import spock.lang.Specification

class FilteringApiIteratorSpec extends Specification {

    def rpcClient = new DefaultRpcClient(null)
    def objectMapper = TestingCommons.objectMapper()
    def ethereumTargets = new QuorumBasedMethods(objectMapper, Chain.ETHEREUM)

    def "Verifies labels"() {
        setup:
        List<EthereumUpstream> upstreams = [
                [test: "foo"],
                [test: "bar"],
                [test: "foo", test2: "baz"],
                [test: "foo"],
                [test: "baz"]
        ].collect {
            new EthereumUpstream(
                    Chain.ETHEREUM,
                    new EthereumApi(rpcClient, objectMapper, Chain.ETHEREUM, ethereumTargets),
                    (EthereumWs) null,
                    new UpstreamsConfig.Options(),
                    new NodeDetailsList.NodeDetails(1, UpstreamsConfig.Labels.fromMap(it)),
                    ethereumTargets
            )
        }
        def matcher = new Selector.LabelMatcher("test", ["foo"])
        upstreams.forEach {
            it.setLag(0)
            it.setStatus(UpstreamAvailability.OK)
        }
        when:
        def iter = new FilteringApiIterator(upstreams, 0, matcher, 1)
        then:
        iter.hasNext()
        iter.next() == upstreams[0].api
        iter.hasNext()
        iter.next() == upstreams[2].api
        iter.hasNext()
        iter.next() == upstreams[3].api
        !iter.hasNext()

        when:
        iter = new FilteringApiIterator(upstreams, 1, matcher, 1)
        then:
        iter.hasNext()
        iter.next() == upstreams[2].api
        iter.hasNext()
        iter.next() == upstreams[3].api
        iter.hasNext()
        iter.next() == upstreams[0].api
        !iter.hasNext()

        when:
        iter = new FilteringApiIterator(upstreams, 1,  matcher, 2)
        then:
        iter.hasNext()
        iter.next() == upstreams[2].api
        iter.hasNext()
        iter.next() == upstreams[3].api
        iter.hasNext()
        iter.next() == upstreams[0].api
        iter.hasNext()
        iter.next() == upstreams[2].api
        iter.hasNext()
        iter.next() == upstreams[3].api
        iter.hasNext()
        iter.next() == upstreams[0].api
        !iter.hasNext()
    }
}
