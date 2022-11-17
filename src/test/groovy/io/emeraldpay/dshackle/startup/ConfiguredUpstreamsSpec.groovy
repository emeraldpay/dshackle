package io.emeraldpay.dshackle.startup

import io.emeraldpay.dshackle.FileResolver
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.monitoring.ingresslog.CurrentIngressLogWriter
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
                currentUpstreams, Stub(FileResolver), Stub(UpstreamsConfig), Stub(CurrentIngressLogWriter)
        )
        def methods = new UpstreamsConfig.Methods(
                [
                        new UpstreamsConfig.Method("foo_bar", null, null),
                        new UpstreamsConfig.Method("foo_bar", "not_empty", null)
                ] as Set,
                [] as Set
        )
        def upstream = new UpstreamsConfig.Upstream()
        upstream.methods = methods
        when:
        def act = configurer.buildMethods(upstream, Chain.ETHEREUM)
        then:
        act instanceof ManagedCallMethods
        act.createQuorumFor("foo_bar") instanceof NonEmptyQuorum
    }

    def "Got static response from extra methods"() {
        setup:
        def currentUpstreams = Mock(CurrentMultistreamHolder) {
            _ * getDefaultMethods(Chain.ETHEREUM) >> new DefaultEthereumMethods(Chain.ETHEREUM)
        }
        def configurer = new ConfiguredUpstreams(
                currentUpstreams, Stub(FileResolver), Stub(UpstreamsConfig), Stub(CurrentIngressLogWriter)
        )
        def methods = new UpstreamsConfig.Methods(
                [
                        new UpstreamsConfig.Method("foo_bar", null, "static_response")
                ] as Set,
                [] as Set
        )
        def upstream = new UpstreamsConfig.Upstream()
        upstream.methods = methods
        when:
        def act = configurer.buildMethods(upstream, Chain.ETHEREUM)
        then:
        act instanceof ManagedCallMethods
        new String(act.executeHardcoded("foo_bar")) == "\"static_response\""
    }
}
