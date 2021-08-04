package io.emeraldpay.dshackle.startup

import io.emeraldpay.dshackle.FileResolver
import io.emeraldpay.dshackle.cache.CachesFactory
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.quorum.NonEmptyQuorum
import io.emeraldpay.dshackle.upstream.CurrentMultistreamHolder
import io.emeraldpay.dshackle.upstream.calls.DefaultEthereumMethods
import io.emeraldpay.dshackle.upstream.calls.ManagedCallMethods
import io.emeraldpay.grpc.Chain
import spock.lang.Specification

class ConfiguredUpstreamsSpec extends Specification {

    def "Applied  quorum to extra methods"() {
        setup:
        def currentUpstreams = Mock(CurrentMultistreamHolder) {
            _ * getDefaultMethods(Chain.ETHEREUM) >> new DefaultEthereumMethods(Chain.ETHEREUM)
        }
        def configurer = new ConfiguredUpstreams(
                currentUpstreams, Stub(FileResolver), Stub(UpstreamsConfig), Stub(CachesFactory)
        )
        def methods = new UpstreamsConfig.Methods(
                [
                        new UpstreamsConfig.Method("foo_bar", null),
                        new UpstreamsConfig.Method("foo_bar", "not_empty")
                ] as Set,
                [] as Set
        )
        def upstream = new UpstreamsConfig.Upstream()
        upstream.methods = methods
        when:
        def act = configurer.buildMethods(upstream, Chain.ETHEREUM)
        then:
        act instanceof ManagedCallMethods
        act.getQuorumFor("foo_bar") instanceof NonEmptyQuorum
    }
}
