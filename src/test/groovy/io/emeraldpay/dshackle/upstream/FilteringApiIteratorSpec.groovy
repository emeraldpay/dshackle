package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.rpc.DefaultRpcClient
import spock.lang.Specification

class FilteringApiIteratorSpec extends Specification {

    def rpcClient = new DefaultRpcClient(null)
    def objectMapper = TestingCommons.objectMapper()

    def "Verifies labels"() {
        setup:
        def upstreams = [
               new EthereumUpstream(
                       Chain.ETHEREUM,
                       new EthereumApi(rpcClient, objectMapper, Chain.ETHEREUM),
                       (EthereumWs)null,
                       new UpstreamsConfig.Options(),
                       new NodeDetailsList.NodeDetails(1, UpstreamsConfig.Labels.fromMap([test: "foo"]))
               ),
               new EthereumUpstream(
                       Chain.ETHEREUM,
                       new EthereumApi(rpcClient, objectMapper, Chain.ETHEREUM),
                       (EthereumWs)null,
                       new UpstreamsConfig.Options(),
                       new NodeDetailsList.NodeDetails(1, UpstreamsConfig.Labels.fromMap([test: "bar"]))
               ),
               new EthereumUpstream(
                       Chain.ETHEREUM,
                       new EthereumApi(rpcClient, objectMapper, Chain.ETHEREUM),
                       (EthereumWs)null,
                       new UpstreamsConfig.Options(),
                       new NodeDetailsList.NodeDetails(1, UpstreamsConfig.Labels.fromMap([test: "foo", test2: "baz"]))
               ),
               new EthereumUpstream(
                       Chain.ETHEREUM,
                       new EthereumApi(rpcClient, objectMapper, Chain.ETHEREUM),
                       (EthereumWs)null,
                       new UpstreamsConfig.Options(),
                       new NodeDetailsList.NodeDetails(1, UpstreamsConfig.Labels.fromMap([test: "foo"]))
               ),
               new EthereumUpstream(
                       Chain.ETHEREUM,
                       new EthereumApi(rpcClient, objectMapper, Chain.ETHEREUM),
                       (EthereumWs)null,
                       new UpstreamsConfig.Options(),
                       new NodeDetailsList.NodeDetails(1, UpstreamsConfig.Labels.fromMap([test: "baz"]))
               )
        ]
        def matcher = new Selector.LabelMatcher("test", ["foo"])
        upstreams.forEach {
            it.setStatus(UpstreamAvailability.OK)
        }
        when:
        def iter = new FilteringApiIterator(upstreams, 3, 0, matcher)
        then:
        iter.hasNext()
        iter.next() == upstreams[0].api
        iter.hasNext()
        iter.next() == upstreams[2].api
        iter.hasNext()
        iter.next() == upstreams[3].api
        !iter.hasNext()

        when:
        iter = new FilteringApiIterator(upstreams, 2, 1, matcher)
        then:
        iter.hasNext()
        iter.next() == upstreams[2].api
        iter.hasNext()
        iter.next() == upstreams[3].api
        !iter.hasNext()

        when:
        iter = new FilteringApiIterator(upstreams, 3, 1, matcher)
        then:
        iter.hasNext()
        iter.next() == upstreams[2].api
        iter.hasNext()
        iter.next() == upstreams[3].api
        iter.hasNext()
        iter.next() == upstreams[0].api
        !iter.hasNext()
    }
}
