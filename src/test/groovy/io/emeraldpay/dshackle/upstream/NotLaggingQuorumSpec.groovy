package io.emeraldpay.dshackle.upstream

import spock.lang.Specification

class NotLaggingQuorumSpec extends Specification {

    def "Resolves if no lag"() {
        setup:
        def up = Mock(Upstream)
        def value = "foo".getBytes()
        def quorum = new NotLaggingQuorum(1)

        when:
        quorum.record(value, up)
        then:
        1 * up.getLag() >> 0
        quorum.isResolved()
        quorum.result == value
    }

    def "Resolves if ok lag"() {
        setup:
        def up = Mock(Upstream)
        def value = "foo".getBytes()
        def quorum = new NotLaggingQuorum(1)

        when:
        quorum.record(value, up)
        then:
        1 * up.getLag() >> 1
        quorum.isResolved()
        quorum.result == value
    }

    def "Ignores if lags"() {
        setup:
        def up = Mock(Upstream)
        def value = "foo".getBytes()
        def quorum = new NotLaggingQuorum(1)

        when:
        quorum.record(value, up)
        then:
        1 * up.getLag() >> 2
        !quorum.isResolved()
    }
}
