/**
 * Copyright (c) 2019 ETCDEV GmbH
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
package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.startup.QuorumForLabels
import org.codehaus.groovy.runtime.DefaultGroovyMethods
import spock.lang.Specification

class QuorumForLabelsSpec extends Specification {

    def "Adds new node"() {
        setup:
        def list = new QuorumForLabels()
        when:
        list.add(new QuorumForLabels.QuorumItem(1, asLabels([foo: "bar"])))
        then:
        list.all.size() == 1
        with(list.all.get(0)) {
            quorum == 1
            DefaultGroovyMethods.equals(labels, [foo: "bar"])
        }

        when:
        list.add(new QuorumForLabels.QuorumItem(2, asLabels([foo: "not-bar"])))
        then:
        list.all.size() == 2
        with(list.all.get(0)) {
            quorum == 1
            DefaultGroovyMethods.equals(labels, [foo: "bar"])
        }
        with(list.all.get(1)) {
            quorum == 2
            DefaultGroovyMethods.equals(labels, [foo: "not-bar"])
        }

        when:
        list.add(new QuorumForLabels.QuorumItem(1, asLabels([foo: "bar", baz: "baz"])))
        then:
        list.all.size() == 3
        with(list.all.get(0)) {
            quorum == 1
            DefaultGroovyMethods.equals(labels, [foo: "bar"])
        }
        with(list.all.get(1)) {
            quorum == 2
            DefaultGroovyMethods.equals(labels, [foo: "not-bar"])
        }
        with(list.all.get(2)) {
            quorum == 1
            DefaultGroovyMethods.equals(labels, [foo: "bar", baz: "baz"])
        }
    }

    def "Updates existing node"() {
        setup:
        def list = new QuorumForLabels()
        list.add(new QuorumForLabels.QuorumItem(1, asLabels([foo: "bar"])))
        list.add(new QuorumForLabels.QuorumItem(1, asLabels([baz: "true"])))

        when:
        list.add(new QuorumForLabels.QuorumItem(2, asLabels([baz: "true"])))
        then:
        list.all.size() == 2
        with(list.all.get(0)) {
            quorum == 1
            DefaultGroovyMethods.equals(labels, [foo: "bar"])
        }
        with(list.all.get(1)) {
            quorum == 3
            DefaultGroovyMethods.equals(labels, [baz: "true"])
        }
    }

    def "Applies all from another list"() {
        setup:
        def list1 = new QuorumForLabels()
        list1.add(new QuorumForLabels.QuorumItem(1, asLabels([foo: "bar"])))
        list1.add(new QuorumForLabels.QuorumItem(2, asLabels([baz: "true"])))
        list1.add(new QuorumForLabels.QuorumItem(3, asLabels([baz: "true", bar: "bar"])))

        def list2 = new QuorumForLabels()
        list1.add(new QuorumForLabels.QuorumItem(4, asLabels([foo: "bar"])))
        list1.add(new QuorumForLabels.QuorumItem(5, asLabels([baz: "true", bar: "bar"])))
        list1.add(new QuorumForLabels.QuorumItem(6, asLabels([bar: "bar"])))

        def list = new QuorumForLabels()
        when:
        list.add(list1)
        list.add(list2)
        def nodes = list.all.toSorted { it.quorum }
        then:
        list.all.size() == 4
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
