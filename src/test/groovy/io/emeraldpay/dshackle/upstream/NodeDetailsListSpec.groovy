package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.config.UpstreamsConfig
import org.codehaus.groovy.runtime.DefaultGroovyMethods
import spock.lang.Specification

class NodeDetailsListSpec extends Specification {

    def "Adds new node"() {
        setup:
        def list = new NodeDetailsList()
        when:
        list.add(new NodeDetailsList.NodeDetails(1, asLabels([foo: "bar"])))
        then:
        list.nodes.size() == 1
        with(list.nodes.get(0)) {
            quorum == 1
            DefaultGroovyMethods.equals(labels, [foo: "bar"])
        }

        when:
        list.add(new NodeDetailsList.NodeDetails(2, asLabels([foo: "not-bar"])))
        then:
        list.nodes.size() == 2
        with(list.nodes.get(0)) {
            quorum == 1
            DefaultGroovyMethods.equals(labels, [foo: "bar"])
        }
        with(list.nodes.get(1)) {
            quorum == 2
            DefaultGroovyMethods.equals(labels, [foo: "not-bar"])
        }

        when:
        list.add(new NodeDetailsList.NodeDetails(1, asLabels([foo: "bar", baz: "baz"])))
        then:
        list.nodes.size() == 3
        with(list.nodes.get(0)) {
            quorum == 1
            DefaultGroovyMethods.equals(labels, [foo: "bar"])
        }
        with(list.nodes.get(1)) {
            quorum == 2
            DefaultGroovyMethods.equals(labels, [foo: "not-bar"])
        }
        with(list.nodes.get(2)) {
            quorum == 1
            DefaultGroovyMethods.equals(labels, [foo: "bar", baz: "baz"])
        }
    }

    def "Updates existing node"() {
        setup:
        def list = new NodeDetailsList()
        list.add(new NodeDetailsList.NodeDetails(1, asLabels([foo: "bar"])))
        list.add(new NodeDetailsList.NodeDetails(1, asLabels([baz: "true"])))

        when:
        list.add(new NodeDetailsList.NodeDetails(2, asLabels([baz: "true"])))
        then:
        list.nodes.size() == 2
        with(list.nodes.get(0)) {
            quorum == 1
            DefaultGroovyMethods.equals(labels, [foo: "bar"])
        }
        with(list.nodes.get(1)) {
            quorum == 3
            DefaultGroovyMethods.equals(labels, [baz: "true"])
        }
    }

    def "Applies all from another list"() {
        setup:
        def list1 = new NodeDetailsList()
        list1.add(new NodeDetailsList.NodeDetails(1, asLabels([foo: "bar"])))
        list1.add(new NodeDetailsList.NodeDetails(2, asLabels([baz: "true"])))
        list1.add(new NodeDetailsList.NodeDetails(3, asLabels([baz: "true", bar: "bar"])))

        def list2 = new NodeDetailsList()
        list1.add(new NodeDetailsList.NodeDetails(4, asLabels([foo: "bar"])))
        list1.add(new NodeDetailsList.NodeDetails(5, asLabels([baz: "true", bar: "bar"])))
        list1.add(new NodeDetailsList.NodeDetails(6, asLabels([bar: "bar"])))

        def list = new NodeDetailsList()
        when:
        list.add(list1)
        list.add(list2)
        def nodes = list.nodes.toSorted { it.quorum }
        then:
        list.nodes.size() == 4
        with(nodes.get(0)) {
            quorum == 2
            DefaultGroovyMethods.equals(labels, [baz: "true"])
        }
        with(nodes.get(1)) {
            quorum == 1 + 4
            DefaultGroovyMethods.equals(labels, [foo: "bar"])
        }
        with(nodes.get(2)) {
            quorum == 6
            DefaultGroovyMethods.equals(labels, [bar: "bar"])
        }
        with(nodes.get(3)) {
            quorum == 3 + 5
            DefaultGroovyMethods.equals(labels, [baz: "true", bar: "bar"])
        }
    }

    // --------


    UpstreamsConfig.Labels asLabels(Map<String, String> values) {
        UpstreamsConfig.Labels result = new UpstreamsConfig.Labels()
        values.entrySet().forEach {
            result.put(it.key, it.value)
        }
        return result
    }
}
