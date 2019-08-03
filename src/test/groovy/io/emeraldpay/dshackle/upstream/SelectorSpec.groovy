package io.emeraldpay.dshackle.upstream

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.dshackle.config.UpstreamsConfig
import spock.lang.Specification

class SelectorSpec extends Specification {

    private BlockchainOuterClass.LabelSelector.Builder selectLabel1 = BlockchainOuterClass.LabelSelector.newBuilder()
            .setName("foo").addAllValue(["bar"])
    private BlockchainOuterClass.Selector selectLabel1Selector = BlockchainOuterClass.Selector.newBuilder()
            .setLabelSelector(selectLabel1).build()

    private BlockchainOuterClass.LabelSelector.Builder selectLabel2 = BlockchainOuterClass.LabelSelector.newBuilder()
            .setName("baz").addAllValue(["bar"])
    private BlockchainOuterClass.Selector selectLabel2Selector = BlockchainOuterClass.Selector.newBuilder()
            .setLabelSelector(selectLabel2).build()

    def "Convert nothing"() {
        when:
        def act = Selector.convertToMatcher(null)
        then:
        act.class == Selector.EmptyMatcher
    }

    def "Convert LABEL match"() {
        when:
        def act = Selector.convertToMatcher(
                selectLabel1Selector
        )
        then:
        act instanceof Selector.LabelMatcher
        with((Selector.LabelMatcher)act) {
            name == "foo"
            values == ["bar"]
        }
    }

    def "Convert EXISTS match"() {
        when:
        def act = Selector.convertToMatcher(
                BlockchainOuterClass.Selector.newBuilder()
                        .setExistsSelector(
                                BlockchainOuterClass.ExistsSelector.newBuilder()
                                        .setName("foo")
                        ).build()
        )
        then:
        act instanceof Selector.ExistsMatcher
        with((Selector.ExistsMatcher)act) {
            name == "foo"
        }
    }

    def "Convert NOT match"() {
        when:
        def act = Selector.convertToMatcher(
                BlockchainOuterClass.Selector.newBuilder()
                        .setNotSelector(
                                BlockchainOuterClass.NotSelector.newBuilder()
                                        .setSelector(selectLabel1Selector).build()
                        )
                        .build()
        )
        then:
        act instanceof Selector.NotMatcher
        with((Selector.NotMatcher)act) {
            matcher instanceof Selector.LabelMatcher
            with((Selector.LabelMatcher)matcher) {
                name == "foo"
                values == ["bar"]
            }
        }
    }

    def "Convert OR match"() {
        when:
        def act = Selector.convertToMatcher(
                BlockchainOuterClass.Selector.newBuilder()
                        .setOrSelector(
                                BlockchainOuterClass.OrSelector.newBuilder()
                                        .addAllSelectors([selectLabel1Selector, selectLabel2Selector]).build()
                        )
                        .build()
        )
        then:
        act instanceof Selector.OrMatcher
        with((Selector.OrMatcher)act) {
            matchers.size() == 2
            with((Selector.LabelMatcher)matchers[0]) {
                name == "foo"
                values == ["bar"]
            }
            with((Selector.LabelMatcher)matchers[1]) {
                name == "baz"
                values == ["bar"]
            }
        }
    }

    def "Convert AND match"() {
        when:
        def act = Selector.convertToMatcher(
                BlockchainOuterClass.Selector.newBuilder()
                        .setAndSelector(
                                BlockchainOuterClass.AndSelector.newBuilder()
                                        .addAllSelectors([selectLabel1Selector, selectLabel2Selector]).build()
                        )
                        .build()
        )
        then:
        act instanceof Selector.AndMatcher
        with((Selector.AndMatcher)act) {
            matchers.size() == 2
            with((Selector.LabelMatcher)matchers[0]) {
                name == "foo"
                values == ["bar"]
            }
            with((Selector.LabelMatcher)matchers[1]) {
                name == "baz"
                values == ["bar"]
            }
        }
    }

    def "Convert LABEL AND NOT LABEl match"() {
        setup:
        def label1 = selectLabel1Selector
        def label2 = selectLabel2Selector
        def notLabel2 = BlockchainOuterClass.Selector.newBuilder()
                .setNotSelector(BlockchainOuterClass.NotSelector.newBuilder().setSelector(label2).build())
                .build()
        def and = BlockchainOuterClass.AndSelector.newBuilder()
                .addAllSelectors([
                        label1, notLabel2
                ]).build()
        when:
        def act = Selector.convertToMatcher(
                BlockchainOuterClass.Selector.newBuilder()
                        .setAndSelector(and)
                        .build()
        )
        then:
        act instanceof Selector.AndMatcher
        with((Selector.AndMatcher)act) {
            matchers.size() == 2
            with((Selector.LabelMatcher)matchers[0]) {
                name == "foo"
                values == ["bar"]
            }
            matchers[1] instanceof Selector.NotMatcher
            with((Selector.NotMatcher)matchers[1]) {
                matcher instanceof Selector.LabelMatcher
                with((Selector.LabelMatcher)matcher) {
                    name == "baz"
                    values == ["bar"]
                }
            }
        }
    }

    def "LABEL matches single label"() {
        setup:
        def matcher = new Selector.LabelMatcher("test", ["foo"])

        expect:
        matcher.matches(UpstreamsConfig.Labels.fromMap(maps))

        where:
        maps << [
                [test: "foo"],
                [test: "foo", test2: "bar"],
                [test2: "foo", test: "foo"],
        ]
    }

    def "LABEL matches one label two values"() {
        setup:
        def matcher = new Selector.LabelMatcher("test", ["foo", "bar"])

        expect:
        matcher.matches(UpstreamsConfig.Labels.fromMap(maps))

        where:
        maps << [
                [test: "foo"],
                [test: "foo", test2: "bar"],
                [test2: "foo", test: "bar"],
        ]
    }

    def "AND matches one label"() {
        setup:
        def matcher = new Selector.AndMatcher(
                [
                new Selector.LabelMatcher("test", ["foo", "bar"])
                ]
        )

        expect:
        matcher.matches(UpstreamsConfig.Labels.fromMap(maps))

        where:
        maps << [
                [test: "foo"],
                [test: "foo", test2: "bar"],
                [test2: "foo", test: "bar"],
        ]
    }

    def "AND matches two labels"() {
        setup:
        def matcher = new Selector.AndMatcher(
                [
                        new Selector.LabelMatcher("test", ["foo", "bar"]),
                        new Selector.LabelMatcher("test2", ["baz"])
                ]
        )

        expect:
        matcher.matches(UpstreamsConfig.Labels.fromMap(maps))

        where:
        maps << [
                [test: "foo", test2: "baz"],
                [test: "foo", test3: "bar", test2: "baz"],
                [test3: "foo", test: "bar", test2: "baz"],
        ]
    }

    def "OR matches two labels"() {
        setup:
        def matcher = new Selector.OrMatcher(
                [
                        new Selector.LabelMatcher("test", ["foo", "bar"]),
                        new Selector.LabelMatcher("test2", ["baz"])
                ]
        )

        expect:
        matcher.matches(UpstreamsConfig.Labels.fromMap(maps))

        where:
        maps << [
                [test: "foo", test2: "baz"],
                [test: "foo", test3: "bar", test2: "baz"],
                [test3: "foo", test: "bar", test2: "baz"],
                [test2: "baz"],
                [test: "bar"],
                [test: "foo"],
        ]
    }

    def "AND NOT matches two labels"() {
        setup:
        def matcher = new Selector.AndMatcher(
                [
                        new Selector.LabelMatcher("test", ["foo", "bar"]),
                        new Selector.NotMatcher(
                                new Selector.LabelMatcher("test2", ["baz"])
                        )
                ]
        )

        expect:
        matcher.matches(UpstreamsConfig.Labels.fromMap(maps))

        where:
        maps << [
                [test: "foo", test2: "not_baz"],
                [test: "foo", test3: "bar", test2: "not_baz"],
                [test: "foo", test3: "bar"],
                [test3: "foo", test: "bar", test2: "not_baz"],
                [test3: "foo", test: "bar"],
                [test: "bar"],
                [test: "foo"],
        ]
    }
}
