package io.emeraldpay.dshackle.startup

import brave.Tracing
import brave.grpc.GrpcTracing
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.FileResolver
import io.emeraldpay.dshackle.config.ChainsConfig
import io.emeraldpay.dshackle.config.CompressionConfig
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.quorum.NotNullQuorum
import io.emeraldpay.dshackle.upstream.CallTargetsHolder
import io.emeraldpay.dshackle.upstream.calls.ManagedCallMethods
import org.springframework.context.ApplicationEventPublisher
import reactor.core.scheduler.Schedulers
import spock.lang.Specification

import java.util.concurrent.Executors

class ConfiguredUpstreamsSpec extends Specification {

    def "Applied  quorum to extra methods"() {
        setup:
        def callTargetsHolder = new CallTargetsHolder()
        def configurer = new ConfiguredUpstreams(
                Stub(FileResolver),
                Stub(UpstreamsConfig),
                Stub(CompressionConfig),
                callTargetsHolder,
                Mock(ApplicationEventPublisher),
                Executors.newFixedThreadPool(1),
                ChainsConfig.default(),
                GrpcTracing.create(Tracing.newBuilder().build()),
                Schedulers.parallel(),
                null,
                Schedulers.parallel(),
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
        act.createQuorumFor("foo_bar") instanceof NotNullQuorum
    }

    def "Got static response from extra methods"() {
        setup:
        def callTargetsHolder = new CallTargetsHolder()
        def configurer = new ConfiguredUpstreams(
                Stub(FileResolver),
                Stub(UpstreamsConfig),
                Stub(CompressionConfig),
                callTargetsHolder,
                Mock(ApplicationEventPublisher),
                Executors.newFixedThreadPool(1),
                ChainsConfig.default(),
                GrpcTracing.create(Tracing.newBuilder().build()),
                Schedulers.parallel(),
                null,
                Schedulers.parallel(),
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

    def "Calculate node-id"() {
        setup:
        def callTargetsHolder = new CallTargetsHolder()
        def configurer = new ConfiguredUpstreams(
                Stub(FileResolver),
                Stub(UpstreamsConfig),
                Stub(CompressionConfig),
                callTargetsHolder,
                Mock(ApplicationEventPublisher),
                Executors.newFixedThreadPool(1),
                ChainsConfig.default(),
                GrpcTracing.create(Tracing.newBuilder().build()),
                Schedulers.parallel(),
                null,
                Schedulers.parallel(),
        )
        expect:
        configurer.getHash(node, src) == expected

        where:
        node | src | expected
        1 | "" | 1
        9 | "hohoho" | 9
        null | "hohoho" | 120
    }

    def "Calculate node-id conflicting results"() {
        setup:
        def callTargetsHolder = new CallTargetsHolder()
        def configurer = new ConfiguredUpstreams(
                Stub(FileResolver),
                Stub(UpstreamsConfig),
                Stub(CompressionConfig),
                callTargetsHolder,
                Mock(ApplicationEventPublisher),
                Executors.newFixedThreadPool(1),
                ChainsConfig.default(),
                GrpcTracing.create(Tracing.newBuilder().build()),
                Schedulers.parallel(),
                null,
                Schedulers.parallel(),
        )
        when:
        def h1 = configurer.getHash(null, "hohoho")
        def h2 = configurer.getHash(null, "hohoho")
        def h3 = configurer.getHash(null, "hohoho")
        def h4 = configurer.getHash(null, "hohoho")
        def h5 = configurer.getHash(null, "hohoho")

        then:
        h1 == (byte)120
        h2 == (byte)-120
        h3 == (byte)-9
        h4 == (byte)8
        h5 == (byte)-128
    }

    def "Supporting method groups"() {
        setup:
        def callTargetsHolder = new CallTargetsHolder()
        def configurer = new ConfiguredUpstreams(
                Stub(FileResolver),
                Stub(UpstreamsConfig),
                Stub(CompressionConfig),
                callTargetsHolder,
                Mock(ApplicationEventPublisher),
                Executors.newFixedThreadPool(1),
                ChainsConfig.default(),
                GrpcTracing.create(Tracing.newBuilder().build()),
                Schedulers.parallel(),
                null,
                Schedulers.parallel(),
        )
        def methodsGroup = new UpstreamsConfig.MethodGroups(
                ["filter"] as Set,
                [] as Set
        )
        def upstream = new UpstreamsConfig.Upstream()
        upstream.methodGroups = methodsGroup
        when:
        def act = configurer.buildMethods(upstream, Chain.ETHEREUM)
        then:
        act instanceof ManagedCallMethods
        act.supportedMethods.findAll {it.containsIgnoreCase("filter")}.size() == 6
    }
}
